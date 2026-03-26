"""
timiron_cloud_briefing.py — Timiron Daily Briefing (v2)

Pulls load log directly from Cadiz Ops OneDrive via Graph API.
Scans Outlook emails for rail swap, maintenance, and carrier data.
Builds dark-themed HTML briefing + Excel attachments, sends via Gmail.

All month-specific constants live in config.yaml — no code changes needed month to month.
"""

import os, json, re, sys, shutil, smtplib, base64, io, tempfile, calendar
from datetime import date, timedelta, datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from pathlib import Path

import requests
import pandas as pd
import openpyxl
import yaml

# ════════════════════════════════════════════════════════════════════════════
# LOAD CONFIG
# ════════════════════════════════════════════════════════════════════════════

SCRIPT_DIR = Path(__file__).parent
CONFIG_PATH = SCRIPT_DIR / "config.yaml"

with open(CONFIG_PATH, encoding='utf-8') as f:
    CFG = yaml.safe_load(f)

# ════════════════════════════════════════════════════════════════════════════
# ENV VARS — from GitHub Secrets
# ════════════════════════════════════════════════════════════════════════════

MS_GRAPH_REFRESH_TOKEN = os.environ.get('MS_GRAPH_REFRESH_TOKEN', '')
MS_GRAPH_CLIENT_ID     = os.environ.get('MS_GRAPH_CLIENT_ID', '')
GMAIL_ADDRESS          = os.environ.get('GMAIL_ADDRESS', 'tyk216@gmail.com')
GMAIL_APP_PASS         = os.environ.get('GMAIL_APP_PASS', '')
RECIPIENTS             = os.environ.get('RECIPIENTS', 'tylerk@timironmp.com,robk@timirontrading.com').split(',')

TEMPLATE_DIR = SCRIPT_DIR / "templates"

GRAPH_BASE = "https://graph.microsoft.com/v1.0"
TOKEN_URL  = "https://login.microsoftonline.com/common/oauth2/v2.0/token"
ACCESS_TOKEN = None

# ════════════════════════════════════════════════════════════════════════════
# DYNAMIC MONTH HELPERS
# ════════════════════════════════════════════════════════════════════════════

def current_month_info():
    """Return dict with dynamic month constants."""
    today = date.today()
    year, month = today.year, today.month
    days_in_month = calendar.monthrange(year, month)[1]
    month_key = f"{year}-{month:02d}"

    # Fixed cost for this month
    costs = CFG.get('monthly_costs', {})
    fixed_cost = costs.get(month_key, costs.get('default', 250000))

    # Prior month info
    if month == 1:
        prev_key = f"{year-1}-12"
    else:
        prev_key = f"{year}-{month-1:02d}"
    prior = CFG.get('prior_months', {}).get(prev_key, {})

    # YTD baseline
    ytd = CFG.get('ytd_baseline', {})

    # Pump hours for month
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
    """Tiered throughput revenue calculation."""
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
# AUTH — Microsoft Graph OAuth2
# ════════════════════════════════════════════════════════════════════════════

def get_access_token():
    global ACCESS_TOKEN
    r = requests.post(TOKEN_URL, data={
        "client_id":     MS_GRAPH_CLIENT_ID,
        "grant_type":    "refresh_token",
        "refresh_token": MS_GRAPH_REFRESH_TOKEN,
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
# GRAPH HELPERS — email search, OneDrive file access
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
# FETCH LOAD LOG FROM ONEDRIVE — direct file access via Graph API
# ════════════════════════════════════════════════════════════════════════════

def fetch_load_log_from_onedrive():
    """Download the Master Load Log directly from Cadiz Ops OneDrive.
    Returns (excel_bytes, filename, last_modified_dt) or (None, None, None).
    """
    od_cfg = CFG.get('onedrive', {})
    user_email = od_cfg.get('cadiz_ops_user', 'cadiz_ops@timirontrading.com')
    folder_path = od_cfg.get('load_log_folder', '/Timiron Cadiz Ops')
    pattern = od_cfg.get('load_log_pattern', 'MASTER COPY').lower()

    print(f"  Looking for load log in {user_email} OneDrive...")

    # List files in the Cadiz Ops folder
    # URL-encode the folder path for the Graph API
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
        # Fallback: try email attachment method
        return fetch_load_log_from_email()

    items = r.json().get("value", [])
    # Find the most recent load log Excel file
    for item in items:
        name = item.get("name", "")
        if (pattern in name.lower()
                and name.lower().endswith(('.xlsx', '.xls'))
                and 'load log' in name.lower()):
            file_id = item["id"]
            modified = item.get("lastModifiedDateTime", "")
            print(f"    Found: {name}")
            print(f"    Last modified: {modified}")

            # Download the file content
            dl_url = f"{GRAPH_BASE}/users/{user_email}/drive/items/{file_id}/content"
            dl = requests.get(dl_url, headers={"Authorization": f"Bearer {ACCESS_TOKEN}"}, timeout=120)
            if dl.ok:
                print(f"    Downloaded: {len(dl.content):,} bytes")
                # Parse last modified datetime
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
    """Fallback: download load log from cadiz.ops LOGS email attachment."""
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
# FETCH OPS DATA FROM EMAIL — rail swaps, updates, carrier projections
# ════════════════════════════════════════════════════════════════════════════

def fetch_email_ops_data():
    """Scan Outlook for operational updates. Returns dict.
    Graceful: returns partial data on any failure, never raises.
    """
    today = date.today()
    yesterday = today - timedelta(days=1)
    today_str = today.strftime('%m.%d.%y')
    yesterday_str = yesterday.strftime('%m.%d.%y')

    email_cfg = CFG.get('email', {})
    maint_keywords = email_cfg.get('maintenance_keywords', [
        'pump', 'repair', 'replace', 'fix', 'broke', 'leak', 'down',
        'maintenance', 'welding', 'valve', 'hose', 'motor', 'pressure', 'gauge'
    ])

    result = {
        "switch_start": None,
        "switch_end": None,
        "loaded_cars_out": 0,
        "empty_cars_in": 0,
        "maintenance_notes": [],
        "carrier_projections": {},
        "email_errors": [],
    }

    # --- RAIL SWAP email ---
    try:
        print("  Searching for RAIL SWAP email...")
        msgs = search_emails(email_cfg.get('rail_swap_search', 'from:cadiz.ops subject:RAIL'))
        for msg in msgs:
            subj = msg.get("subject", "")
            if today_str in subj or yesterday_str in subj:
                body = get_body_text(msg)
                start_m = re.search(r'START\s+TIME\s+(\d{1,2}:\d{2}\s*[AP]M)', body, re.IGNORECASE)
                end_m = re.search(r'END\s+TIME\s+(\d{1,2}:\d{2}\s*[AP]M)', body, re.IGNORECASE)
                loaded_m = re.search(r'(\d+)\s+LOADED\s+CARS?\s+SENT', body, re.IGNORECASE)
                empty_m = re.search(r'(\d+)\s+EMPTY\s+CARS?\s+PUSHED', body, re.IGNORECASE)
                if start_m: result["switch_start"] = start_m.group(1).strip()
                if end_m:   result["switch_end"] = end_m.group(1).strip()
                if loaded_m: result["loaded_cars_out"] = int(loaded_m.group(1))
                if empty_m:  result["empty_cars_in"] = int(empty_m.group(1))
                print(f"    Found RAIL SWAP: {result['switch_start']} -> {result['switch_end']}")
                break
    except Exception as e:
        print(f"  Warning: Rail swap search failed: {e}")
        result["email_errors"].append(f"Rail swap search failed: {e}")

    # --- UPDATE email ---
    try:
        print("  Searching for UPDATE email...")
        msgs = search_emails(email_cfg.get('update_search', 'from:cadiz.ops subject:UPDATE'))
        for msg in msgs:
            subj = msg.get("subject", "")
            if today_str in subj or yesterday_str in subj:
                time_m = re.search(r'(\d{1,2}:\d{2}\s*[AP]M)\s+UPDATE', subj, re.IGNORECASE)
                if time_m:
                    result["switch_end"] = time_m.group(1).strip()
                    print(f"    UPDATE resume time: {result['switch_end']}")
                body = get_body_text(msg)
                if body.strip():
                    for line in body.strip().split('\n'):
                        line = line.strip()
                        if line and len(line) > 10 and any(kw in line.lower() for kw in maint_keywords):
                            result["maintenance_notes"].append(line[:200])
                break
    except Exception as e:
        print(f"  Warning: Update search failed: {e}")
        result["email_errors"].append(f"Update search failed: {e}")

    # --- Carrier projections ---
    carriers_cfg = CFG.get('carriers', [])
    for carrier in carriers_cfg:
        cname = carrier['name']
        search_q = carrier.get('email_search')
        if not search_q:
            result["carrier_projections"][cname] = {
                "trucks": 0, "proj_bbls": 0, "note": "No email search configured", "responded": False
            }
            continue

        try:
            print(f"  Searching for {cname} carrier reply...")
            msgs = search_emails(search_q)
            trucks = 0
            responded = False
            for msg in msgs:
                recv = msg.get("receivedDateTime", "")
                if today.strftime('%Y-%m-%d') in recv[:10]:
                    body = get_body_text(msg)
                    truck_m = re.search(r'(\d+)\s+(?:planned|trucks?|loads?|scheduled)', body, re.IGNORECASE)
                    if not truck_m:
                        truck_m = re.search(r'(?:have|running|sending|doing)\s+(\d+)', body, re.IGNORECASE)
                    if truck_m:
                        trucks = int(truck_m.group(1))
                        responded = True
                        print(f"    {cname}: {trucks} trucks")
                    break

            result["carrier_projections"][cname] = {
                "trucks": trucks,
                "proj_bbls": 0,  # will be filled after we have carrier avgs
                "note": "" if responded else "No response today",
                "responded": responded,
            }
        except Exception as e:
            print(f"  Warning: {cname} carrier search failed: {e}")
            result["email_errors"].append(f"{cname} search failed: {e}")
            result["carrier_projections"][cname] = {
                "trucks": 0, "proj_bbls": 0, "note": f"Search error", "responded": False
            }

    # Fill carriers without email search
    for carrier in carriers_cfg:
        cname = carrier['name']
        if cname not in result["carrier_projections"]:
            result["carrier_projections"][cname] = {
                "trucks": 0, "proj_bbls": 0, "note": "No response", "responded": False
            }

    return result

# ════════════════════════════════════════════════════════════════════════════
# PARSE LOAD LOG — fully dynamic, no hardcoded months
# ════════════════════════════════════════════════════════════════════════════

def parse_load_log(excel_bytes, mi):
    """Parse Master Load Log Excel. mi = month_info dict from current_month_info().
    Returns comprehensive data dict.
    """
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

    # Filter to current month
    month_data = df[df['Date'] >= mi['month_start']].copy()
    if month_data.empty:
        raise ValueError(f"No {mi['month_name']} {mi['year']} data in load log.")

    yesterday = date.today() - timedelta(days=1)
    mtd_data = month_data[month_data['Date'] <= yesterday]

    if mtd_data.empty:
        raise ValueError(f"No data through {yesterday} in load log.")

    yday = month_data[month_data['Date'] == yesterday]
    yday_count = len(yday)
    print(f"  Yesterday: {yesterday}  ({yday_count} loads)")
    mtd_days = sorted(mtd_data['Date'].unique())
    print(f"  MTD: {len(mtd_days)} days  ({min(mtd_days)} -- {max(mtd_days)})")

    # ── Pump utilization (yesterday) ─────────────────────────────────────
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

    # ── MTD pump hours (for Excel templates) ─────────────────────────────
    pump_mtd_hrs = {}
    for pump in pumps_cfg:
        prefix = pump['bol_prefix']
        hrs = mtd_data[mtd_data['BOL_prefix'] == prefix]['pump_mins'].sum() / 60
        pump_mtd_hrs[pump['name']] = round(hrs, 2)

    # ── MTD aggregates ───────────────────────────────────────────────────
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

    # ── Carrier actuals from yesterday ───────────────────────────────────
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

    # ── Rolling carrier averages (MTD) ───────────────────────────────────
    carrier_rolling_avgs = {}
    if 'Carrier' in month_data.columns:
        mtd_no_split_c = mtd_no_split.copy()
        for carrier_name, grp in mtd_no_split_c.groupby('Carrier'):
            normalized = carrier_name_map.get(carrier_name, carrier_name)
            carrier_days = grp['Date'].nunique()
            carrier_total_trucks = len(grp)
            carrier_total_bbls = mtd_data[mtd_data['Carrier'] == carrier_name]['Metered'].sum()
            if carrier_total_trucks > 0:
                carrier_rolling_avgs[normalized] = {
                    'avg_bbls_per_truck': round(carrier_total_bbls / carrier_total_trucks, 1),
                    'avg_trucks_per_day': round(carrier_total_trucks / days_actual, 1),
                    'total_trucks': carrier_total_trucks,
                    'total_bbls': round(carrier_total_bbls, 1),
                }

    # ── Dynamic weekly breakdowns ────────────────────────────────────────
    weeks = []
    month_start = mi['month_start']
    # Build week boundaries (Mon-Sun)
    week_num = 1
    d_cursor = month_start
    while d_cursor.month == mi['month']:
        # Find start of this week (current cursor)
        week_start = d_cursor
        # Find end of this week (next Sunday or end of month)
        days_to_sun = 6 - d_cursor.weekday()  # weekday: Mon=0, Sun=6
        week_end = min(d_cursor + timedelta(days=days_to_sun),
                       date(mi['year'], mi['month'], mi['days_in_month']))

        # Get data for this week (only completed days up through yesterday)
        wk_data = mtd_data[(mtd_data['Date'] >= week_start) & (mtd_data['Date'] <= min(week_end, yesterday))]
        if not wk_data.empty:
            wk_no_split = wk_data[~wk_data['Split Load'].astype(str).str.contains('Split #2', na=False)]
            wk_daily_bbls = wk_data.groupby('Date')['Metered'].sum()
            wk_daily_trucks = wk_no_split.groupby('Date').size()
            wk_total_bbls = wk_daily_bbls.sum()
            wk_total_trucks = wk_daily_trucks.sum()
            wk_days = len(wk_daily_bbls)

            # Daily detail for each day in the week
            daily_detail = []
            for dd in sorted(wk_data['Date'].unique()):
                dd_data = wk_data[wk_data['Date'] == dd]
                dd_no_split = dd_data[~dd_data['Split Load'].astype(str).str.contains('Split #2', na=False)]
                dd_bbls = dd_data['Metered'].sum()
                dd_trucks = len(dd_no_split)
                daily_detail.append({
                    'date': dd,
                    'bbls': round(dd_bbls, 2),
                    'trucks': dd_trucks,
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

    # ── Day-over-day trend (last 5 days) ─────────────────────────────────
    recent_dates = sorted(mtd_data['Date'].unique())[-5:]
    day_trend = []
    for dd in recent_dates:
        dd_data = mtd_data[mtd_data['Date'] == dd]
        dd_no_split = dd_data[~dd_data['Split Load'].astype(str).str.contains('Split #2', na=False)]
        dd_bbls = dd_data['Metered'].sum()
        dd_trucks = len(dd_no_split)
        day_trend.append({
            'date': dd,
            'bbls': round(dd_bbls, 2),
            'trucks': dd_trucks,
        })

    # ── Weekend vs weekday split ─────────────────────────────────────────
    mtd_data_wday = mtd_data.copy()
    mtd_data_wday['weekday'] = mtd_data_wday['Date'].apply(lambda x: x.weekday())
    mtd_data_wday['is_weekend'] = mtd_data_wday['weekday'].isin([5, 6])

    mtd_no_split_wday = mtd_no_split.copy()
    mtd_no_split_wday['weekday'] = mtd_no_split_wday['Date'].apply(lambda x: x.weekday())
    mtd_no_split_wday['is_weekend'] = mtd_no_split_wday['weekday'].isin([5, 6])

    weekday_data = mtd_data_wday[~mtd_data_wday['is_weekend']]
    weekend_data = mtd_data_wday[mtd_data_wday['is_weekend']]
    weekday_trucks_data = mtd_no_split_wday[~mtd_no_split_wday['is_weekend']]
    weekend_trucks_data = mtd_no_split_wday[mtd_no_split_wday['is_weekend']]

    weekday_days = weekday_data['Date'].nunique()
    weekend_days = weekend_data['Date'].nunique()

    wday_wkend = {
        'weekday': {
            'days': weekday_days,
            'total_bbls': round(weekday_data['Metered'].sum(), 1),
            'avg_bbls': round(weekday_data['Metered'].sum() / weekday_days, 1) if weekday_days > 0 else 0,
            'total_trucks': len(weekday_trucks_data),
            'avg_trucks': round(len(weekday_trucks_data) / weekday_days, 1) if weekday_days > 0 else 0,
        },
        'weekend': {
            'days': weekend_days,
            'total_bbls': round(weekend_data['Metered'].sum(), 1),
            'avg_bbls': round(weekend_data['Metered'].sum() / weekend_days, 1) if weekend_days > 0 else 0,
            'total_trucks': len(weekend_trucks_data),
            'avg_trucks': round(len(weekend_trucks_data) / weekend_days, 1) if weekend_days > 0 else 0,
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

# ════════════════════════════════════════════════════════════════════════════
# UPDATE EXCEL TEMPLATES
# ════════════════════════════════════════════════════════════════════════════

def safe_write(ws, row, col, val):
    cell = ws.cell(row, col)
    if cell.__class__.__name__ != 'MergedCell':
        cell.value = val


def find_template(name_fragment):
    exact = TEMPLATE_DIR / f"Timiron_{name_fragment}.xlsx"
    if exact.exists():
        print(f"  Template: {exact.name}")
        return str(exact)
    raise FileNotFoundError(f"\nTemplate not found: {exact}\nLooked in: {TEMPLATE_DIR}")


def update_dashboard(template_path, d, mi, output_path):
    shutil.copy(template_path, output_path)
    wb = openpyxl.load_workbook(output_path)
    ws = wb['Operations Dashboard']
    dt = d['yesterday_date'].strftime('%b %#d')
    r = 16
    ws.cell(r, 3).value = d['proj_rev'];      ws.cell(r, 5).value = d['ebitda']
    ws.cell(r, 6).value = d['ebitda'];         ws.cell(r, 7).value = d['proj_bbls']
    ws.cell(r, 8).value = d['avg_bbls'];       ws.cell(r, 9).value = d['avg_trucks']
    ws.cell(r, 10).value = d['proj_trucks'];   ws.cell(r, 11).value = round(d['proj_rev'] / d['proj_bbls'], 2) if d['proj_bbls'] else 0
    ws.cell(r, 12).value = round(mi['fixed_cost'] / d['proj_bbls'], 3) if d['proj_bbls'] else 0
    ws.cell(r, 13).value = round(d['ebitda'] / d['proj_bbls'], 3) if d['proj_bbls'] else 0
    ws.cell(r, 14).value = d['pump_ute_combined']
    ws.cell(r, 15).value = d['rail_cap']
    ws.cell(3, 1).value = (
        f"Source: Actual P&Ls, Master Load Logs, Payroll Files, Trafigura Invoices  |  "
        f"Forecast: {mi['month_abbr']} {mi['year']} based on {d['days_actual']}-day actuals (through {dt})  |  "
        f"{mi['month_abbr']} {mi['year']} costs from {mi['month_abbr']} P&L Forecast tab"
    )
    ws.cell(18, 1).value = (
        f"\u2020 {mi['month_abbr']} {mi['year']} = FORECAST based on {d['days_actual']}-day actuals "
        f"({d['avg_bbls']:.1f} bbls/day avg) + steady run-rate through {mi['month_abbr']} {mi['days_in_month']}. "
        f"Actuals through {dt} from Cadiz Ops OneDrive load log. "
        "Dec 2025 cost is negative due to 71k payroll reversal.\n\n"
        "*  Adj Pump Util % = pump runtime hours \u00f7 true available hours (21 hrs/day).\n\n"
        f"**  % of Rail Cap/Day = avg bbls/day \u00f7 {CFG['operations']['rail_cap_daily_bbls']:,} bbl daily rail ceiling."
    )

    # Pump runtime sheet
    pr = wb['Pump Runtime']
    month_label = f"{mi['month_abbr']} {mi['year']}"
    pumps_cfg = CFG.get('operations', {}).get('pumps', [])
    for r2 in range(1, 20):
        if pr.cell(r2, 1).value and month_label in str(pr.cell(r2, 1).value):
            col = 3
            for pump in pumps_cfg:
                hrs = d['pump_mtd_hrs'].get(pump['name'], 0)
                pr.cell(r2, col).value = hrs
                pr.cell(r2, col + 1).value = round(mi['pump_hrs_month'] - hrs, 2)
                pr.cell(r2, col + 2).value = round(hrs / mi['pump_hrs_month'], 3)
                col += 3
            pr.cell(r2, col).value = f"All {len(pumps_cfg)} pumps active \u00b7 Partial month thru {dt}"
            break

    wb.save(output_path)
    print(f"  Dashboard saved: {os.path.basename(output_path)}")


def update_external_report(template_path, d, mi, output_path):
    shutil.copy(template_path, output_path)
    wb = openpyxl.load_workbook(output_path)
    bbt = round(d['avg_bbls'] / d['avg_trucks'], 1) if d['avg_trucks'] > 0 else 0
    yb = mi['ytd_bbls_baseline'] + d['proj_bbls']
    yt = mi['ytd_trucks_baseline'] + d['proj_trucks']
    dt = d['yesterday_date'].strftime('%b %#d')
    prior = mi['prior_month']
    prior_name = mi['prior_month_key']

    to = wb['Terminal Overview']
    safe_write(to, 21, 3, d['proj_bbls']);       safe_write(to, 21, 4, round(d['avg_bbls'], 0))
    safe_write(to, 21, 5, d['proj_trucks']);      safe_write(to, 21, 6, d['avg_trucks'])
    safe_write(to, 21, 7, bbt);                   safe_write(to, 21, 8, d['pump_ute_combined'])
    safe_write(to, 21, 9, d['rail_cap']);          safe_write(to, 21, 10, round(1 - d['rail_cap'], 3))
    safe_write(to, 5, 1, round(yb, 0));           safe_write(to, 5, 3, round(yt, 0))

    om = wb['Operational Metrics']
    pumps_cfg = CFG.get('operations', {}).get('pumps', [])
    col = 3
    for pump in pumps_cfg:
        hrs = d['pump_mtd_hrs'].get(pump['name'], 0)
        safe_write(om, 15, col, hrs)
        safe_write(om, 15, col + 1, round(hrs / mi['pump_hrs_month'], 3))
        col += 2
    total_pump_hrs = sum(d['pump_mtd_hrs'].values())
    safe_write(om, 15, col, round(total_pump_hrs, 1))
    safe_write(om, 15, col + 1, d['pump_ute_combined'])

    ca = wb['Capacity Analysis']
    safe_write(ca, 29, 6, round(d['avg_trucks'], 0))
    safe_write(ca, 29, 7, round(d['avg_bbls'], 0))
    safe_write(ca, 29, 9, round(d['avg_bbls'] * mi['days_in_month'], 0))

    kt = wb['Key Takeaways']
    prior_str = ""
    if prior:
        prior_str = (f"{calendar.month_abbr[int(prior_name[-2:])]}: "
                     f"{prior.get('avg_trucks_per_day', 0):.1f} trucks/day, "
                     f"{prior.get('avg_bbls_per_day', 0):,.0f} bbls/day, "
                     f"{prior.get('total_bbls', 0):,.0f} bbls for the month. ")
    safe_write(kt, 8, 3,
        f"{prior_str}"
        f"{mi['month_abbr']} {mi['year']} (through {dt}): {d['avg_trucks']:.1f} trucks/day, "
        f"{d['avg_bbls']:.1f} bbls/day run rate, {d['proj_bbls']:,.0f} bbls projected. "
        f"Total since April 2025: {yb:,.0f} bbls across {yt:,.0f} truck loads."
    )

    wb.save(output_path)
    print(f"  External report saved: {os.path.basename(output_path)}")

# ════════════════════════════════════════════════════════════════════════════
# BUILD EMAIL HTML — fully dynamic
# ════════════════════════════════════════════════════════════════════════════

def calc_switch_duration(start_str, end_str):
    try:
        fmt = '%I:%M%p'
        s = datetime.strptime(start_str.upper().replace(' ', ''), fmt)
        e = datetime.strptime(end_str.upper().replace(' ', ''), fmt)
        diff = (e - s).seconds // 60
        return (str(diff // 60) + "hr " + str(diff % 60) + "min") if diff >= 60 else (str(diff) + "min")
    except Exception:
        return ""


def build_cadiz_section(ops_data, carrier_actuals, carrier_rolling_avgs):
    """Build Cadiz Ops Activity + Carrier Performance HTML."""
    switch_start = ops_data.get('switch_start')
    switch_end = ops_data.get('switch_end')
    loaded_out = ops_data.get('loaded_cars_out', 0)
    empty_in = ops_data.get('empty_cars_in', 0)
    carrier_proj = ops_data.get('carrier_projections', {})
    maint_notes = ops_data.get('maintenance_notes', [])
    email_errors = ops_data.get('email_errors', [])

    has_content = any([switch_start, carrier_proj, carrier_actuals, maint_notes, email_errors])
    if not has_content:
        return ""

    # Switch
    duration_str = ""
    if switch_start and switch_end:
        duration_str = calc_switch_duration(switch_start, switch_end)

    switch_html = ""
    if switch_start:
        cars_str = ""
        if loaded_out: cars_str += f" &nbsp;\u00b7&nbsp; {loaded_out} loaded out"
        if empty_in:   cars_str += f" / {empty_in} empty in"
        dur = f" &nbsp;\u00b7&nbsp; {duration_str}" if duration_str else ""
        switch_html = f'<div class="kv"><span class="lbl">Rail Switch</span><span class="val">{switch_start} \u2192 {switch_end or "?"}{dur}{cars_str}</span></div>'

    # Maintenance
    maint_html = ""
    for note in maint_notes:
        maint_html += f'<div class="kv"><span class="lbl">Maintenance</span><span class="val" style="color:#90caf9;">{note[:150]}</span></div>'

    # Email errors
    error_html = ""
    for err in email_errors:
        error_html += f'<div class="kv"><span class="lbl" style="color:#ef5350;">Email Error</span><span class="val" style="color:#ef5350;font-size:11px;">{err}</span></div>'

    # Carrier table
    all_carriers = [c['name'] for c in CFG.get('carriers', [])]
    total_proj_trucks = 0
    total_actual_trucks = 0
    total_actual_bbls = 0
    rows = ""

    for c in all_carriers:
        proj = carrier_proj.get(c, {})
        actual = carrier_actuals.get(c, {})
        rolling = carrier_rolling_avgs.get(c, {})
        proj_trucks = proj.get('trucks', 0)
        actual_trucks = actual.get('trucks', 0)
        actual_bbls = actual.get('bbls', 0)
        responded = proj.get('responded', False)

        # Projection column
        if proj_trucks > 0:
            proj_str = str(proj_trucks)
            total_proj_trucks += proj_trucks
        elif not responded and proj.get('note'):
            proj_str = '<span style="color:#ef5350;font-size:10px;">No reply</span>'
        else:
            proj_str = '<span style="color:#666;">\u2014</span>'

        # Actual column
        if actual_trucks > 0:
            actual_str = str(actual_trucks)
            bbls_str = f"{actual_bbls:,.0f}"
            total_actual_trucks += actual_trucks
            total_actual_bbls += actual_bbls
        else:
            actual_str = '<span style="color:#666;">0</span>'
            bbls_str = '<span style="color:#666;">\u2014</span>'

        # MTD avg column
        mtd_avg_str = ""
        if rolling:
            mtd_avg_str = f"{rolling['avg_trucks_per_day']:.1f} / {rolling['avg_bbls_per_truck']:.0f}"
        else:
            mtd_avg_str = '<span style="color:#666;">\u2014</span>'

        # Variance
        if proj_trucks > 0 and actual_trucks > 0:
            var = actual_trucks - proj_trucks
            if var > 0:
                var_str = f'<span style="color:#4caf50;">+{var}</span>'
            elif var < 0:
                var_str = f'<span style="color:#ef5350;">{var}</span>'
            else:
                var_str = '<span style="color:#888;">0</span>'
        elif proj_trucks == 0 and actual_trucks > 0:
            var_str = '<span style="color:#888;">\u2014</span>'
        elif proj_trucks > 0 and actual_trucks == 0:
            var_str = f'<span style="color:#ef5350;">-{proj_trucks}</span>'
        else:
            var_str = '<span style="color:#666;">\u2014</span>'

        rows += f'<tr><td>{c}</td><td>{proj_str}</td><td>{actual_str}</td><td>{bbls_str}</td><td style="font-size:10px;">{mtd_avg_str}</td><td>{var_str}</td></tr>'

    # Total row
    proj_total_str = str(total_proj_trucks) if total_proj_trucks > 0 else '\u2014'
    total_var = total_actual_trucks - total_proj_trucks if total_proj_trucks > 0 else 0
    if total_proj_trucks > 0:
        if total_var > 0:
            var_total = f'<span style="color:#4caf50;">+{total_var}</span>'
        elif total_var < 0:
            var_total = f'<span style="color:#ef5350;">{total_var}</span>'
        else:
            var_total = '0'
    else:
        var_total = '\u2014'

    total_row = f'<tr class="tot"><td>Total</td><td>{proj_total_str}</td><td>{total_actual_trucks}</td><td>{total_actual_bbls:,.0f}</td><td></td><td>{var_total}</td></tr>'

    carrier_html = f"""
  <div class="sec-head" style="margin-top:10px;">\U0001f69b Carrier Performance</div>
  <table>
    <tr><th>Carrier</th><th>Projected</th><th>Actual</th><th>BBLs</th><th style="font-size:9px;">MTD Avg<br>(trk/day / bbl/trk)</th><th>Var</th></tr>
    {rows}
    {total_row}
  </table>"""

    return f"""
<div class="section">
  <div class="sec-head">\U0001f4e1 Cadiz Ops Activity</div>
  {switch_html}
  {maint_html}
  {error_html}
  {carrier_html}
</div>"""


def build_email_html(d, mi, dash_name, ext_name, cadiz_section="", data_source_info=""):
    pu = d['pump_ute']
    dt_str = d['yesterday_date'].strftime('%B %#d, %Y')
    today_str = date.today().strftime('%A, %B %#d, %Y')
    pump_avail = CFG.get('operations', {}).get('pump_available_hrs', 21)
    num_pumps = len(CFG.get('operations', {}).get('pumps', []))

    yday_bbls = sum(v['bbls'] for v in pu.values())
    yday_trucks = sum(v['loads'] for v in pu.values())
    yday_splits = sum(v['splits'] for v in pu.values())
    yday_rt = sum(v['runtime'] for v in pu.values())
    combined_ute = yday_rt / (pump_avail * num_pumps) * 100 if yday_rt > 0 else 0
    bbl_hr_comb = yday_bbls / yday_rt if yday_rt > 0 else 0
    bbl_per_truck = yday_bbls / yday_trucks if yday_trucks > 0 else 0

    vs_run = (yday_bbls - d['avg_bbls']) / d['avg_bbls'] * 100 if d['avg_bbls'] > 0 else 0

    # Prior month comparison
    prior = mi.get('prior_month', {})
    prior_total = prior.get('total_bbls', 0)
    prior_avg = prior.get('avg_bbls_per_day', 0)
    prior_name_short = calendar.month_abbr[int(mi['prior_month_key'][-2:])] if mi.get('prior_month_key') else "Prior"
    vs_prior_bbls = d['proj_bbls'] - prior_total if prior_total else 0
    mtd_vs_prior = (d['avg_bbls'] - prior_avg) / prior_avg * 100 if prior_avg > 0 else 0

    def badge(val):
        c = '#4caf50' if val >= 0 else '#ef5350'
        s = '+' if val >= 0 else ''
        return f'<span style="color:{c}">{s}{val:.1f}%</span>'

    def badge_abs(val):
        c = '#4caf50' if val >= 0 else '#ef5350'
        s = '+' if val >= 0 else ''
        return f'<span style="color:{c}">{s}{val:,.0f} BBLs</span>'

    # ── Day-over-day trend ───────────────────────────────────────────────
    trend_rows = ""
    trend = d.get('day_trend', [])
    for i, day in enumerate(trend):
        dd = day['date']
        day_name = dd.strftime('%a')
        day_label = f"{dd.strftime('%b %#d')} ({day_name})"
        bbls = day['bbls']
        trucks = day['trucks']
        bpt = bbls / trucks if trucks > 0 else 0
        vs = (bbls - d['avg_bbls']) / d['avg_bbls'] * 100 if d['avg_bbls'] > 0 else 0
        # Day-over-day change
        if i > 0:
            prev_bbls = trend[i - 1]['bbls']
            dod = (bbls - prev_bbls) / prev_bbls * 100 if prev_bbls > 0 else 0
            dod_col = '#4caf50' if dod >= 0 else '#ef5350'
            dod_sign = '+' if dod >= 0 else ''
            dod_str = f'<span style="color:{dod_col}">{dod_sign}{dod:.1f}%</span>'
        else:
            dod_str = '<span style="color:#666;">\u2014</span>'

        vs_col = '#4caf50' if vs >= 0 else '#ef5350'
        vs_sign = '+' if vs >= 0 else ''
        is_weekend = dd.weekday() in [5, 6]
        row_style = ' style="background:#1e1e2a;"' if is_weekend else ''
        trend_rows += f'<tr{row_style}><td>{day_label}</td><td>{bbls:,.0f}</td><td>{trucks}</td><td>{bpt:.1f}</td><td style="color:{vs_col}">{vs_sign}{vs:.1f}%</td><td>{dod_str}</td></tr>'

    # ── Weekly breakdown ─────────────────────────────────────────────────
    weeks = d.get('weeks', [])
    week_summary_rows = ""
    for wk in weeks:
        wk_label = f"Wk {wk['week_num']} ({wk['start'].strftime('%b %#d')}\u2013{wk['end'].strftime('%#d')})"
        wk_avg = wk['avg_bbls']
        wk_vs = (wk_avg - d['avg_bbls']) / d['avg_bbls'] * 100 if d['avg_bbls'] > 0 else 0
        vs_col = '#4caf50' if wk_vs >= 0 else '#ef5350'
        vs_sign = '+' if wk_vs >= 0 else ''
        bpt = wk['total_bbls'] / wk['total_trucks'] if wk['total_trucks'] > 0 else 0
        week_summary_rows += (
            f'<tr><td>{wk_label}</td><td>{wk["total_bbls"]:,.0f}</td><td>{wk["total_trucks"]}</td>'
            f'<td>{wk["days"]}</td><td>{wk_avg:,.0f}</td><td>{bpt:.1f}</td>'
            f'<td style="color:{vs_col}">{vs_sign}{wk_vs:.1f}%</td></tr>'
        )

    # ── Weekend / weekday split ──────────────────────────────────────────
    ww = d.get('wday_wkend', {})
    wd = ww.get('weekday', {})
    we = ww.get('weekend', {})
    ww_html = ""
    if wd.get('days', 0) > 0 and we.get('days', 0) > 0:
        diff_pct = (we['avg_bbls'] - wd['avg_bbls']) / wd['avg_bbls'] * 100 if wd['avg_bbls'] > 0 else 0
        diff_col = '#4caf50' if diff_pct >= 0 else '#ef5350'
        diff_sign = '+' if diff_pct >= 0 else ''
        ww_html = f"""
  <div style="margin-top:8px;font-size:11px;color:#777;border-top:1px solid #2e2e2e;padding-top:6px;">
    Weekday avg: {wd['avg_bbls']:,.0f} bbls ({wd['avg_trucks']:.1f} trucks) over {wd['days']} days
    &nbsp;\u00b7&nbsp; Weekend avg: {we['avg_bbls']:,.0f} bbls ({we['avg_trucks']:.1f} trucks) over {we['days']} days
    &nbsp;\u00b7&nbsp; Weekend vs weekday: <span style="color:{diff_col}">{diff_sign}{diff_pct:.1f}%</span>
  </div>"""

    # ── Pump table rows ──────────────────────────────────────────────────
    pump_rows = ""
    pumps_cfg = CFG.get('operations', {}).get('pumps', [])
    for pump in pumps_cfg:
        pname = pump['name']
        p = pu.get(pname, {})
        pump_rows += (
            f'<tr><td>{pname}</td><td>{p.get("loads", 0)}</td><td>{p.get("splits", 0)}</td>'
            f'<td>{p.get("runtime", 0)} hrs</td><td>{p.get("ute", 0)}%</td>'
            f'<td>{p.get("bbls", 0):,.0f}</td><td>{p.get("bbl_hr", 0):.0f}</td></tr>'
        )

    # ── Flags ────────────────────────────────────────────────────────────
    flag_cfg = CFG.get('flags', {})
    flags = []

    # Soft streak
    soft_threshold = flag_cfg.get('soft_day_threshold_pct', 5)
    streak_len = flag_cfg.get('soft_streak_days', 4)
    if len(trend) >= streak_len:
        recent = trend[-streak_len:]
        if all((r['bbls'] - d['avg_bbls']) / d['avg_bbls'] * 100 < -soft_threshold for r in recent if d['avg_bbls'] > 0):
            date_range = f"{recent[0]['date'].strftime('%b %#d')}\u2013{recent[-1]['date'].strftime('%#d')}"
            flags.append(('red', f"{date_range} all running below run rate \u2014 {streak_len}+ soft days in a row. Monitor dispatch."))

    # Pump imbalance
    imbalance_thresh = flag_cfg.get('pump_imbalance_threshold_pct', 10)
    utes = {k: v['ute'] for k, v in pu.items() if v['runtime'] > 0}
    if len(utes) >= 2:
        max_p = max(utes, key=utes.get)
        min_p = min(utes, key=utes.get)
        if utes[max_p] - utes[min_p] > imbalance_thresh:
            flags.append(('yellow', f"{min_p} ute {utes[min_p]}% vs {max_p} {utes[max_p]}% \u2014 load imbalance."))

    # Low bbls/hr
    low_hr_thresh = flag_cfg.get('low_bbls_per_hr', 430)
    low_hr = [f"{k} at {v['bbl_hr']:.0f}" for k, v in pu.items() if v['bbl_hr'] > 0 and v['bbl_hr'] < low_hr_thresh]
    if low_hr:
        flags.append(('yellow', f"BBLs/hr below avg ({low_hr_thresh}): {', '.join(low_hr)}."))

    # Split count
    split_normal = flag_cfg.get('split_normal_pct', 24)
    split_pct = (yday_splits / yday_trucks * 100) if yday_trucks > 0 else 0
    if yday_trucks > 0:
        if split_pct <= split_normal + 3:
            flags.append(('grn', f"Split count {yday_splits}/{yday_trucks} ({split_pct:.1f}%) \u2014 near historical avg ({split_normal}%)."))
        elif split_pct > split_normal + 10:
            flags.append(('yellow', f"Split count {yday_splits}/{yday_trucks} ({split_pct:.1f}%) \u2014 elevated vs avg ({split_normal}%)."))

    flags_html = ""
    for ftype, msg in flags:
        cls = f' {ftype}' if ftype in ('red', 'grn') else ''
        flags_html += f'<div class="flag{cls}">{msg}</div>'
    flags_html += '<div class="flag grn">\U0001f4ce Both Excel files attached \u2014 open directly in Excel.</div>'

    # Data source info
    source_html = ""
    if data_source_info:
        source_html = f'<div style="color:#555;font-size:10px;margin-top:4px;">{data_source_info}</div>'

    rail_cap_daily = CFG.get('operations', {}).get('rail_cap_daily_bbls', 15000)

    return f"""<!DOCTYPE html>
<html><head><meta charset="UTF-8">
<style>
  body{{background:#1a1a1a;font-family:'Courier New',monospace;color:#e0e0e0;margin:0;padding:20px;}}
  .wrap{{max-width:680px;margin:0 auto;}}
  .title{{color:#fff;font-size:15px;font-weight:bold;border-bottom:1px solid #444;padding-bottom:8px;margin-bottom:6px;}}
  .sub{{color:#666;font-size:11px;margin-bottom:18px;}}
  .section{{background:#242424;border:1px solid #333;border-radius:3px;padding:13px 15px;margin-bottom:12px;}}
  .sec-head{{color:#999;font-size:10px;text-transform:uppercase;letter-spacing:1px;margin-bottom:9px;border-bottom:1px solid #2e2e2e;padding-bottom:5px;}}
  .kv{{display:flex;justify-content:space-between;padding:3px 0;font-size:13px;border-bottom:1px solid #2a2a2a;}}
  .kv:last-child{{border-bottom:none;}}
  .lbl{{color:#888;}}.val{{color:#ddd;font-weight:bold;}}
  table{{width:100%;border-collapse:collapse;font-size:12px;margin-top:2px;}}
  th{{color:#777;font-weight:normal;text-align:left;padding:4px 6px;border-bottom:1px solid #333;font-size:10px;}}
  td{{padding:5px 6px;border-bottom:1px solid #2a2a2a;color:#ccc;}}
  tr.tot td{{color:#fff;font-weight:bold;background:#2d2d2d;border-top:1px solid #444;}}
  .flag{{padding:6px 10px;margin-bottom:6px;border-left:3px solid #FFD100;background:#2a2700;font-size:12px;border-radius:2px;}}
  .flag.grn{{border-left-color:#4caf50;background:#162016;}}
  .flag.red{{border-left-color:#ef5350;background:#2a1515;}}
  .foot{{color:#444;font-size:10px;text-align:center;margin-top:14px;border-top:1px solid #2a2a2a;padding-top:10px;}}
</style>
</head><body><div class="wrap">
<div class="title">\U0001f4ca Timiron Daily Briefing &nbsp;|&nbsp; {CFG['terminal']['name']}</div>
<div class="sub">{today_str} &nbsp;\u00b7&nbsp; Based on {dt_str} data
{source_html}
</div>

<div class="section">
  <div class="sec-head">\U0001f4c5 Yesterday \u2014 {dt_str}</div>
  <div class="kv"><span class="lbl">BBLs</span><span class="val">{yday_bbls:,.2f}</span></div>
  <div class="kv"><span class="lbl">Trucks</span><span class="val">{yday_trucks}</span></div>
  <div class="kv"><span class="lbl">Avg BBLs / Truck</span><span class="val">{bbl_per_truck:.1f}</span></div>
  <div class="kv"><span class="lbl">vs Run Rate Avg</span><span class="val">{badge(vs_run)} &nbsp;({yday_bbls:,.0f} vs {d['avg_bbls']:,.0f} avg)</span></div>
</div>

<div class="section">
  <div class="sec-head">\u2699\ufe0f Pump Utilization \u2014 {dt_str}</div>
  <table>
    <tr><th>Pump</th><th>Loads</th><th>Splits</th><th>Runtime</th><th>Ute %</th><th>BBLs</th><th>BBLs/Hr</th></tr>
    {pump_rows}
    <tr class="tot"><td>Combined</td><td>{yday_trucks}</td><td>{yday_splits}</td><td>{yday_rt:.2f} hrs</td><td>{combined_ute:.1f}%</td><td>{yday_bbls:,.0f}</td><td>{bbl_hr_comb:.0f}</td></tr>
  </table>
  <div style="color:#555;font-size:10px;margin-top:6px;">Ute % = runtime \u00f7 {pump_avail} available hrs/pump (24hr \u2212 3hr rail switch)</div>
</div>

<div class="section">
  <div class="sec-head">\U0001f4c8 5-Day Trend</div>
  <table>
    <tr><th>Date</th><th>BBLs</th><th>Trucks</th><th>BBLs/Trk</th><th>vs Avg</th><th>DoD</th></tr>
    {trend_rows}
  </table>
  {ww_html}
</div>

<div class="section">
  <div class="sec-head">\U0001f4c6 Month-to-Date ({mi['month_abbr']} 1\u2013{d['yesterday_date'].day})</div>
  <div class="kv"><span class="lbl">MTD Actuals</span><span class="val">{d['total_bbls']:,.0f} BBLs</span></div>
  <div class="kv"><span class="lbl">Daily Avg ({d['days_actual']} days)</span><span class="val">{d['avg_bbls']:,.1f} bbls/day</span></div>
  <div class="kv"><span class="lbl">vs {prior_name_short} Avg ({prior_avg:,.0f}/day)</span><span class="val">{badge(mtd_vs_prior)}</span></div>
  <div class="kv"><span class="lbl">% of Rail Cap ({rail_cap_daily:,}/day)</span><span class="val" style="color:#90caf9">{d['rail_cap']*100:.1f}%</span></div>
  <div class="kv"><span class="lbl">Days Remaining</span><span class="val">{d['days_remain']}</span></div>
</div>

<div class="section">
  <div class="sec-head">\U0001f4ca Weekly Breakdown</div>
  <table>
    <tr><th>Week</th><th>Total BBLs</th><th>Trucks</th><th>Days</th><th>Avg/Day</th><th>BBLs/Trk</th><th>vs Avg</th></tr>
    {week_summary_rows}
  </table>
</div>

<div class="section">
  <div class="sec-head">\U0001f4c8 {mi['month_name']} Forecast ({d['days_actual']}-Day Run Rate)</div>
  <div class="kv"><span class="lbl">Run Rate Avg</span><span class="val">{d['avg_bbls']:,.1f} bbls/day</span></div>
  <div class="kv"><span class="lbl">Projected Total BBLs</span><span class="val">{d['proj_bbls']:,.0f}</span></div>
  <div class="kv"><span class="lbl">Projected Trucks</span><span class="val">~{d['proj_trucks']:,.0f}</span></div>
  <div class="kv"><span class="lbl">vs {prior_name_short} Actual ({prior_total:,.0f} BBLs)</span><span class="val">{badge_abs(vs_prior_bbls)}</span></div>
</div>

{cadiz_section}

<div class="section">
  <div class="sec-head">\u26a0\ufe0f Flags</div>
  {flags_html}
</div>

</div>
<div class="foot">
  {CFG['terminal']['company']} \u00b7 {CFG['terminal']['name']}, {CFG['terminal']['location']} \u00b7 Auto-generated<br>
  Data: Cadiz Ops OneDrive (direct) + Outlook email scan \u00b7 {dash_name} \u00b7 {ext_name}
</div>
</div></body></html>"""

# ════════════════════════════════════════════════════════════════════════════
# SEND EMAIL VIA GMAIL SMTP
# ════════════════════════════════════════════════════════════════════════════

def send_via_gmail(subject, html_body, attachment_paths):
    msg = MIMEMultipart('mixed')
    msg['From'] = GMAIL_ADDRESS
    msg['To'] = ', '.join(RECIPIENTS)
    msg['Subject'] = subject
    msg.attach(MIMEText(html_body, 'html'))

    for path in attachment_paths:
        with open(path, 'rb') as f:
            part = MIMEBase('application', 'vnd.openxmlformats-officedocument.spreadsheetml.sheet')
            part.set_payload(f.read())
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', 'attachment', filename=os.path.basename(path))
        msg.attach(part)

    with smtplib.SMTP_SSL('smtp.gmail.com', 465) as server:
        server.login(GMAIL_ADDRESS, GMAIL_APP_PASS)
        server.sendmail(GMAIL_ADDRESS, RECIPIENTS, msg.as_string())

    print(f"  Email sent to {len(RECIPIENTS)} recipients with {len(attachment_paths)} attachments.")

# ════════════════════════════════════════════════════════════════════════════
# MAIN
# ════════════════════════════════════════════════════════════════════════════

def main():
    today = date.today()
    date_str = today.strftime('%#m-%#d-%y')

    print("=" * 62)
    print(f"  Timiron Daily Briefing v2 -- {today.strftime('%B %d, %Y')}")
    print("=" * 62)

    # Validate config
    if not MS_GRAPH_REFRESH_TOKEN:
        print("ERROR: MS_GRAPH_REFRESH_TOKEN not set"); sys.exit(1)
    if not MS_GRAPH_CLIENT_ID:
        print("ERROR: MS_GRAPH_CLIENT_ID not set"); sys.exit(1)
    if not GMAIL_APP_PASS:
        print("ERROR: GMAIL_APP_PASS not set"); sys.exit(1)

    mi = current_month_info()
    print(f"\n  Month: {mi['month_name']} {mi['year']} ({mi['days_in_month']} days)")
    print(f"  Fixed cost: ${mi['fixed_cost']:,.2f}")

    tmpdir = tempfile.mkdtemp(prefix="timiron_")

    # Step 0: Auth
    print("\n[0] Authenticating with Microsoft Graph...")
    if not get_access_token():
        print("  FATAL: Could not authenticate"); sys.exit(1)

    # Step 1: Fetch load log from OneDrive (with email fallback)
    print("\n[1] Fetching load log from Cadiz Ops OneDrive...")
    excel_bytes, excel_filename, last_modified = fetch_load_log_from_onedrive()
    if not excel_bytes:
        print("  FATAL: Could not obtain load log from any source"); sys.exit(1)

    # Check staleness
    data_source_info = f"Source: {excel_filename}"
    if last_modified:
        stale_hrs = CFG.get('flags', {}).get('stale_data_hours', 18)
        age_hrs = (datetime.now(last_modified.tzinfo) - last_modified).total_seconds() / 3600
        mod_str = last_modified.strftime('%b %#d %I:%M %p')
        data_source_info += f" &nbsp;\u00b7&nbsp; Last saved: {mod_str}"
        if age_hrs > stale_hrs:
            data_source_info += f' &nbsp;\u00b7&nbsp; <span style="color:#ef5350;">STALE ({age_hrs:.0f}hrs old)</span>'
            print(f"  WARNING: Load log is {age_hrs:.0f} hours old (threshold: {stale_hrs}hrs)")
        else:
            print(f"  Data age: {age_hrs:.1f} hours (OK)")

    # Step 2: Scan emails for ops data (graceful)
    print("\n[2] Scanning Outlook for operational updates...")
    ops_data = fetch_email_ops_data()
    if ops_data.get('email_errors'):
        print(f"  Partial email data ({len(ops_data['email_errors'])} errors)")
    else:
        print("  Email scan complete")

    # Step 3: Parse load log
    print("\n[3] Parsing load log...")
    d = parse_load_log(excel_bytes, mi)

    # Fill carrier projected bbls using rolling avgs
    for cname, proj in ops_data.get('carrier_projections', {}).items():
        rolling = d['carrier_rolling_avgs'].get(cname, {})
        avg_bpt = rolling.get('avg_bbls_per_truck', 190)
        proj['proj_bbls'] = round(proj['trucks'] * avg_bpt)

    # Step 4: Update Excel templates
    print("\n[4] Updating Operations Dashboard...")
    dash_tpl = find_template("Operations_Dashboard_MASTER")
    dash_out = os.path.join(tmpdir, f"Timiron_Operations_Dashboard_MASTER_{date_str}.xlsx")
    update_dashboard(dash_tpl, d, mi, dash_out)

    print("\n[5] Updating External Report...")
    ext_tpl = find_template("External_Report")
    ext_out = os.path.join(tmpdir, f"Timiron_External_Report_{date_str}.xlsx")
    update_external_report(ext_tpl, d, mi, ext_out)

    # Step 6: Build HTML
    print("\n[6] Building email...")
    cadiz_section = build_cadiz_section(ops_data, d.get('carrier_actuals', {}), d.get('carrier_rolling_avgs', {}))
    dash_name = os.path.basename(dash_out)
    ext_name = os.path.basename(ext_out)
    html_body = build_email_html(d, mi, dash_name, ext_name, cadiz_section, data_source_info)
    print(f"  HTML: {len(html_body):,} bytes")

    # Step 7: Send
    print("\n[7] Sending via Gmail...")
    subject = f"\U0001f4ca Timiron Daily Briefing | {today.strftime('%A, %B %d, %Y')}"
    send_via_gmail(subject, html_body, [dash_out, ext_out])

    print("\n" + "=" * 62)
    print("  DONE")
    print("=" * 62)


if __name__ == "__main__":
    main()
