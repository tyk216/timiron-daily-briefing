"""
parse_loadlog.py — Shared module for parsing the Cadiz Ops Master Load Log.

Used by both timiron_cloud_briefing.py (email) and generate_dashboard_json.py (PWA).
All Graph API auth, OneDrive access, and Excel parsing logic lives here.
"""

import os, re, base64, io, calendar
from datetime import date, timedelta, datetime
from pathlib import Path

import requests
import pandas as pd
import yaml

# ════════════════════════════════════════════════════════════════════════════
# CONFIG
# ════════════════════════════════════════════════════════════════════════════

SCRIPT_DIR = Path(__file__).parent
CONFIG_PATH = SCRIPT_DIR / "config.yaml"

with open(CONFIG_PATH, encoding='utf-8') as f:
    CFG = yaml.safe_load(f)

GRAPH_BASE = "https://graph.microsoft.com/v1.0"
TOKEN_URL  = "https://login.microsoftonline.com/common/oauth2/v2.0/token"
ACCESS_TOKEN = None

# ════════════════════════════════════════════════════════════════════════════
# DYNAMIC MONTH HELPERS
# ════════════════════════════════════════════════════════════════════════════

def current_month_info():
    today = date.today()
    year, month = today.year, today.month
    days_in_month = calendar.monthrange(year, month)[1]
    month_key = f"{year}-{month:02d}"

    costs = CFG.get('monthly_costs', {})
    fixed_cost = costs.get(month_key, costs.get('default', 250000))

    if month == 1:
        prev_key = f"{year-1}-12"
    else:
        prev_key = f"{year}-{month-1:02d}"
    prior = CFG.get('prior_months', {}).get(prev_key, {})

    ytd = CFG.get('ytd_baseline', {})
    pump_hrs_month = days_in_month * 24

    return {
        'year': year,
        'month': month,
        'month_name': calendar.month_name[month],
        'month_abbr': calendar.month_abbr[month],
        'days_in_month': days_in_month,
        'month_key': month_key,
        'month_start': date(year, month, 1),
        'fixed_cost': fixed_cost,
        'pump_hrs_month': pump_hrs_month,
        'prior_month': prior,
        'prior_month_key': prev_key,
        'ytd_bbls_baseline': ytd.get('total_bbls', 0),
        'ytd_trucks_baseline': ytd.get('total_trucks', 0),
    }


def rev_per_day(bbls):
    tiers = CFG.get('revenue_tiers', [])
    rev = 0
    remaining = bbls
    prev_cap = 0
    for tier in tiers:
        if 'up_to_bbls' in tier:
            cap = tier['up_to_bbls']
            bracket = min(remaining, cap - prev_cap)
            rev += max(0, bracket) * tier['rate']
            remaining -= bracket
            prev_cap = cap
        elif 'above' in tier:
            rev += max(0, remaining) * tier['rate']
    return rev

# ════════════════════════════════════════════════════════════════════════════
# AUTH
# ════════════════════════════════════════════════════════════════════════════

def get_access_token(client_id=None, refresh_token=None):
    global ACCESS_TOKEN
    client_id = client_id or os.environ.get('MS_GRAPH_CLIENT_ID', '')
    refresh_token = refresh_token or os.environ.get('MS_GRAPH_REFRESH_TOKEN', '')

    r = requests.post(TOKEN_URL, data={
        "client_id":     client_id,
        "grant_type":    "refresh_token",
        "refresh_token": refresh_token,
        "scope":         "Mail.Read Files.Read.All Sites.Read.All offline_access",
    }, timeout=30)
    if not r.ok:
        print(f"  Token refresh failed: {r.status_code} {r.text[:300]}")
        return False
    data = r.json()
    ACCESS_TOKEN = data["access_token"]
    new_rt = data.get("refresh_token")
    if new_rt:
        print("  New refresh token received (90-day lifetime).")
    print("  Access token acquired.")
    return True


def graph_headers():
    return {"Authorization": f"Bearer {ACCESS_TOKEN}", "Content-Type": "application/json"}

# ════════════════════════════════════════════════════════════════════════════
# GRAPH HELPERS
# ════════════════════════════════════════════════════════════════════════════

def search_emails(search_query, top=5):
    url = f'{GRAPH_BASE}/me/messages'
    params = {
        "$search": f'"{search_query}"',
        "$top": top,
        "$select": "id,subject,from,body,receivedDateTime,hasAttachments",
    }
    r = requests.get(url, headers=graph_headers(), params=params, timeout=30)
    if not r.ok:
        print(f"  Search failed for '{search_query}': {r.status_code} {r.text[:200]}")
        return []
    return r.json().get("value", [])


def get_attachments(message_id):
    url = f'{GRAPH_BASE}/me/messages/{message_id}/attachments'
    r = requests.get(url, headers=graph_headers(), timeout=60)
    if not r.ok:
        print(f"  Get attachments failed: {r.status_code}")
        return []
    return r.json().get("value", [])


def get_body_text(msg):
    body = msg.get("body", {})
    content = body.get("content", "")
    if body.get("contentType") == "html":
        content = re.sub(r'<br\s*/?>', '\n', content, flags=re.IGNORECASE)
        content = re.sub(r'<[^>]+>', '', content)
        content = re.sub(r'&nbsp;', ' ', content)
        content = re.sub(r'&#\d+;', '', content)
    return content.strip()

# ════════════════════════════════════════════════════════════════════════════
# FETCH LOAD LOG
# ════════════════════════════════════════════════════════════════════════════

def fetch_load_log_from_onedrive():
    od_cfg = CFG.get('onedrive', {})
    user_email = od_cfg.get('cadiz_ops_user', 'cadiz_ops@timirontrading.com')
    folder_path = od_cfg.get('load_log_folder', '/Timiron Cadiz Ops')
    pattern = od_cfg.get('load_log_pattern', 'MASTER COPY').lower()

    print(f"  Looking for load log in {user_email} OneDrive...")

    encoded_path = folder_path.strip('/').replace(' ', '%20')
    url = f"{GRAPH_BASE}/users/{user_email}/drive/root:/{encoded_path}:/children"
    params = {
        "$select": "name,id,lastModifiedDateTime,size,file",
        "$top": 100,
        "$orderby": "lastModifiedDateTime desc",
    }

    r = requests.get(url, headers=graph_headers(), params=params, timeout=30)
    if not r.ok:
        print(f"  OneDrive list failed: {r.status_code} {r.text[:300]}")
        return fetch_load_log_from_email()

    items = r.json().get("value", [])
    for item in items:
        name = item.get("name", "")
        if (pattern in name.lower()
                and name.lower().endswith(('.xlsx', '.xls'))
                and 'load log' in name.lower()):
            file_id = item["id"]
            modified = item.get("lastModifiedDateTime", "")
            print(f"    Found: {name}")
            print(f"    Last modified: {modified}")

            dl_url = f"{GRAPH_BASE}/users/{user_email}/drive/items/{file_id}/content"
            dl = requests.get(dl_url, headers={"Authorization": f"Bearer {ACCESS_TOKEN}"}, timeout=120)
            if dl.ok:
                print(f"    Downloaded: {len(dl.content):,} bytes")
                mod_dt = None
                if modified:
                    try:
                        mod_dt = datetime.fromisoformat(modified.replace('Z', '+00:00'))
                    except Exception:
                        pass
                return dl.content, name, mod_dt
            else:
                print(f"    Download failed: {dl.status_code}")
                break

    print("  Could not find load log in OneDrive, trying email fallback...")
    return fetch_load_log_from_email()


def fetch_load_log_from_email():
    print("  Searching for LOGS email from cadiz.ops...")
    search_q = CFG.get('email', {}).get('logs_search', 'from:cadiz.ops subject:LOGS')
    msgs = search_emails(search_q)

    for msg in msgs:
        if not msg.get("hasAttachments"):
            continue
        attachments = get_attachments(msg["id"])
        for att in attachments:
            name = att.get("name", "")
            if name.lower().endswith(('.xlsx', '.xls')) and 'load log' in name.lower():
                content_bytes = att.get("contentBytes")
                if content_bytes:
                    excel_bytes = base64.b64decode(content_bytes)
                    print(f"    Found email attachment: {name} ({len(excel_bytes):,} bytes)")
                    recv = msg.get("receivedDateTime", "")
                    mod_dt = None
                    if recv:
                        try:
                            mod_dt = datetime.fromisoformat(recv.replace('Z', '+00:00'))
                        except Exception:
                            pass
                    return excel_bytes, name, mod_dt

    print("  ERROR: Could not find Master Load Log anywhere")
    return None, None, None

# ════════════════════════════════════════════════════════════════════════════
# PARSE LOAD LOG
# ════════════════════════════════════════════════════════════════════════════

def parse_load_log(excel_bytes, mi):
    df = pd.read_excel(io.BytesIO(excel_bytes), sheet_name='Master_Load_Log', header=0)
    df['Date']       = pd.to_datetime(df['Date']).dt.date
    df['BOL_prefix'] = df['Timiron BOL#'].astype(str).str[:3]
    df['Metered']    = pd.to_numeric(df['Timiron Metered bbls.'], errors='coerce').fillna(0)

    def to_mins(t):
        try:
            s = str(t)
            p = s.split(':')
            return int(p[0]) * 60 + int(p[1])
        except Exception:
            return 0

    df['pump_mins'] = df['Pump Time'].apply(to_mins)

    month_data = df[df['Date'] >= mi['month_start']].copy()
    if month_data.empty:
        raise ValueError(f"No {mi['month_name']} {mi['year']} data in load log.")

    yesterday = date.today() - timedelta(days=1)
    mtd_data = month_data[month_data['Date'] <= yesterday]

    if mtd_data.empty:
        raise ValueError(f"No data through {yesterday} in load log.")

    yday = month_data[month_data['Date'] == yesterday]
    print(f"  Yesterday: {yesterday}  ({len(yday)} loads)")
    mtd_days = sorted(mtd_data['Date'].unique())
    print(f"  MTD: {len(mtd_days)} days  ({min(mtd_days)} -- {max(mtd_days)})")

    # Pump utilization (yesterday)
    pumps_cfg = CFG.get('operations', {}).get('pumps', [])
    pump_avail = CFG.get('operations', {}).get('pump_available_hrs', 21)
    pump_ute = {}
    for pump in pumps_cfg:
        pname = pump['name']
        prefix = pump['bol_prefix']
        p = yday[yday['BOL_prefix'] == prefix]
        splits = p[p['Split Load'].astype(str).str.contains('Split #2', na=False)]
        non_split = p[~p['Split Load'].astype(str).str.contains('Split #2', na=False)]
        runtime = p['pump_mins'].sum() / 60
        bbls = p['Metered'].sum()
        pump_ute[pname] = {
            'loads': len(non_split), 'splits': len(splits),
            'runtime': round(runtime, 2),
            'ute': round(runtime / pump_avail * 100, 1),
            'bbls': round(bbls, 2),
            'bbl_hr': round(bbls / runtime, 0) if runtime > 0 else 0,
        }
    combined_rt = sum(v['runtime'] for v in pump_ute.values())
    num_pumps = len(pumps_cfg)

    # MTD pump hours
    pump_mtd_hrs = {}
    for pump in pumps_cfg:
        hrs = mtd_data[mtd_data['BOL_prefix'] == pump['bol_prefix']]['pump_mins'].sum() / 60
        pump_mtd_hrs[pump['name']] = round(hrs, 2)

    # MTD aggregates
    mtd_no_split = mtd_data[~mtd_data['Split Load'].astype(str).str.contains('Split #2', na=False)]
    daily_trucks = mtd_no_split.groupby('Date').size()
    daily_bbls   = mtd_data.groupby('Date')['Metered'].sum()
    total_bbls   = daily_bbls.sum()
    total_trucks  = daily_trucks.sum()
    days_actual  = len(daily_bbls)
    days_remain  = mi['days_in_month'] - days_actual
    avg_bbls     = total_bbls / days_actual
    avg_trucks   = total_trucks / days_actual
    proj_bbls    = total_bbls + avg_bbls * days_remain
    proj_trucks  = total_trucks + avg_trucks * days_remain
    proj_rev     = rev_per_day(avg_bbls) * mi['days_in_month']
    ebitda       = proj_rev - mi['fixed_cost']
    rail_cap     = avg_bbls / CFG.get('operations', {}).get('rail_cap_daily_bbls', 15000)

    print(f"  MTD BBLs:  {total_bbls:,.2f}  avg {avg_bbls:,.1f}/day")
    print(f"  Projected: {proj_bbls:,.0f} BBLs | {proj_trucks:,.0f} trucks")

    # Carrier actuals (yesterday)
    carrier_name_map = {}
    for c in CFG.get('carriers', []):
        norm_from = c.get('normalize_from', c['name'])
        if norm_from != c['name']:
            carrier_name_map[norm_from] = c['name']

    carrier_actuals = {}
    if 'Carrier' in month_data.columns and not yday.empty:
        yday_no_split = yday[~yday['Split Load'].astype(str).str.contains('Split #2', na=False)]
        for carrier_name, grp in yday_no_split.groupby('Carrier'):
            normalized = carrier_name_map.get(carrier_name, carrier_name)
            actual_trucks = len(grp)
            actual_bbls = round(yday[yday['Carrier'] == carrier_name]['Metered'].sum(), 1)
            carrier_actuals[normalized] = {'trucks': actual_trucks, 'bbls': actual_bbls}

    # Rolling carrier averages (MTD)
    carrier_rolling_avgs = {}
    if 'Carrier' in month_data.columns:
        for carrier_name, grp in mtd_no_split.groupby('Carrier'):
            normalized = carrier_name_map.get(carrier_name, carrier_name)
            carrier_total_trucks = len(grp)
            carrier_total_bbls = mtd_data[mtd_data['Carrier'] == carrier_name]['Metered'].sum()
            if carrier_total_trucks > 0:
                carrier_rolling_avgs[normalized] = {
                    'avg_bbls_per_truck': round(carrier_total_bbls / carrier_total_trucks, 1),
                    'avg_trucks_per_day': round(carrier_total_trucks / days_actual, 1),
                    'total_trucks': carrier_total_trucks,
                    'total_bbls': round(carrier_total_bbls, 1),
                }

    # Dynamic weekly breakdowns
    weeks = []
    month_start = mi['month_start']
    week_num = 1
    d_cursor = month_start
    while d_cursor.month == mi['month']:
        week_start = d_cursor
        days_to_sun = 6 - d_cursor.weekday()
        week_end = min(d_cursor + timedelta(days=days_to_sun),
                       date(mi['year'], mi['month'], mi['days_in_month']))

        wk_data = mtd_data[(mtd_data['Date'] >= week_start) & (mtd_data['Date'] <= min(week_end, yesterday))]
        if not wk_data.empty:
            wk_no_split = wk_data[~wk_data['Split Load'].astype(str).str.contains('Split #2', na=False)]
            wk_daily_bbls = wk_data.groupby('Date')['Metered'].sum()
            wk_daily_trucks = wk_no_split.groupby('Date').size()
            wk_total_bbls = wk_daily_bbls.sum()
            wk_total_trucks = wk_daily_trucks.sum()
            wk_days = len(wk_daily_bbls)

            daily_detail = []
            for dd in sorted(wk_data['Date'].unique()):
                dd_data = wk_data[wk_data['Date'] == dd]
                dd_no_split = dd_data[~dd_data['Split Load'].astype(str).str.contains('Split #2', na=False)]
                daily_detail.append({
                    'date': dd,
                    'bbls': round(dd_data['Metered'].sum(), 2),
                    'trucks': len(dd_no_split),
                })

            weeks.append({
                'week_num': week_num,
                'start': week_start,
                'end': min(week_end, yesterday),
                'total_bbls': round(wk_total_bbls, 2),
                'total_trucks': int(wk_total_trucks),
                'days': wk_days,
                'avg_bbls': round(wk_total_bbls / wk_days, 1) if wk_days > 0 else 0,
                'daily_detail': daily_detail,
            })

        d_cursor = week_end + timedelta(days=1)
        week_num += 1
        if d_cursor.month != mi['month']:
            break

    # Day-over-day trend (last 5 days)
    recent_dates = sorted(mtd_data['Date'].unique())[-5:]
    day_trend = []
    for dd in recent_dates:
        dd_data = mtd_data[mtd_data['Date'] == dd]
        dd_no_split = dd_data[~dd_data['Split Load'].astype(str).str.contains('Split #2', na=False)]
        day_trend.append({
            'date': dd,
            'bbls': round(dd_data['Metered'].sum(), 2),
            'trucks': len(dd_no_split),
        })

    # Weekend vs weekday
    mtd_data_wday = mtd_data.copy()
    mtd_data_wday['is_weekend'] = mtd_data_wday['Date'].apply(lambda x: x.weekday() in [5, 6])
    mtd_no_split_wday = mtd_no_split.copy()
    mtd_no_split_wday['is_weekend'] = mtd_no_split_wday['Date'].apply(lambda x: x.weekday() in [5, 6])

    weekday_data = mtd_data_wday[~mtd_data_wday['is_weekend']]
    weekend_data = mtd_data_wday[mtd_data_wday['is_weekend']]
    weekday_days = weekday_data['Date'].nunique()
    weekend_days = weekend_data['Date'].nunique()

    wday_wkend = {
        'weekday': {
            'days': weekday_days,
            'total_bbls': round(weekday_data['Metered'].sum(), 1),
            'avg_bbls': round(weekday_data['Metered'].sum() / weekday_days, 1) if weekday_days > 0 else 0,
            'total_trucks': len(mtd_no_split_wday[~mtd_no_split_wday['is_weekend']]),
            'avg_trucks': round(len(mtd_no_split_wday[~mtd_no_split_wday['is_weekend']]) / weekday_days, 1) if weekday_days > 0 else 0,
        },
        'weekend': {
            'days': weekend_days,
            'total_bbls': round(weekend_data['Metered'].sum(), 1),
            'avg_bbls': round(weekend_data['Metered'].sum() / weekend_days, 1) if weekend_days > 0 else 0,
            'total_trucks': len(mtd_no_split_wday[mtd_no_split_wday['is_weekend']]),
            'avg_trucks': round(len(mtd_no_split_wday[mtd_no_split_wday['is_weekend']]) / weekend_days, 1) if weekend_days > 0 else 0,
        },
    }

    return {
        'yesterday_date': yesterday,
        'days_actual': days_actual,
        'days_remain': days_remain,
        'total_bbls': round(total_bbls, 2),
        'total_trucks': int(total_trucks),
        'avg_bbls': round(avg_bbls, 1),
        'avg_trucks': round(avg_trucks, 1),
        'proj_bbls': round(proj_bbls, 0),
        'proj_trucks': round(proj_trucks, 0),
        'proj_rev': round(proj_rev, 2),
        'ebitda': round(ebitda, 2),
        'rail_cap': round(rail_cap, 6),
        'pump_ute': pump_ute,
        'pump_ute_combined': round(combined_rt / (pump_avail * num_pumps), 3) if num_pumps > 0 else 0,
        'pump_mtd_hrs': pump_mtd_hrs,
        'carrier_actuals': carrier_actuals,
        'carrier_rolling_avgs': carrier_rolling_avgs,
        'weeks': weeks,
        'day_trend': day_trend,
        'wday_wkend': wday_wkend,
    }
