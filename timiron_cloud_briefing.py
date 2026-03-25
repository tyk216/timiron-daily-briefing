"""
timiron_cloud_briefing.py — Cloud version of Timiron Daily Briefing
Runs on GitHub Actions at 6 AM ET daily.
Uses Microsoft Graph API directly for Outlook email search and attachments.
Sends HTML briefing email via Gmail SMTP.
"""

import os, json, re, sys, smtplib, base64, io
from datetime import date, timedelta, datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import requests
import pandas as pd

# ════════════════════════════════════════════════════════════════════════════
# CONFIG — from GitHub Secrets (environment variables)
# ════════════════════════════════════════════════════════════════════════════

MS_GRAPH_REFRESH_TOKEN = os.environ.get('MS_GRAPH_REFRESH_TOKEN', '')
MS_GRAPH_CLIENT_ID     = os.environ.get('MS_GRAPH_CLIENT_ID', '')
GMAIL_ADDRESS          = os.environ.get('GMAIL_ADDRESS', 'tyk216@gmail.com')
GMAIL_APP_PASS         = os.environ.get('GMAIL_APP_PASS', '')
RECIPIENTS             = os.environ.get('RECIPIENTS', 'tylerk@timironmp.com,robk@timirontrading.com').split(',')

CARRIER_AVGS = {
    'Badlands':          224.6,
    'KAG':               188.9,
    'Prop Logistics':    181.5,
    'BD Oil':            188.2,
    '1st Choice Energy': 183.3,
}

MARCH_DAYS       = 31
RAIL_CAP_DAILY   = 15000
MARCH_FIXED_COST = 244583.52

FEB_AVG_DAILY    = 11200
FEB_TOTAL_BBLS   = 313600
FEB_AVG_UTE      = 43.9

GRAPH_BASE = "https://graph.microsoft.com/v1.0"
TOKEN_URL  = "https://login.microsoftonline.com/common/oauth2/v2.0/token"

# Session-level access token (set once at startup)
ACCESS_TOKEN = None

def rev_per_day(bbls):
    return (min(bbls,5000)*1.30 + max(0,min(bbls-5000,5000))*0.95 + max(0,bbls-10000)*0.75)

# ════════════════════════════════════════════════════════════════════════════
# AUTH — Microsoft Graph OAuth2 refresh token flow
# ════════════════════════════════════════════════════════════════════════════

def get_access_token():
    """Exchange refresh token for a new access token."""
    global ACCESS_TOKEN
    r = requests.post(TOKEN_URL, data={
        "client_id":     MS_GRAPH_CLIENT_ID,
        "grant_type":    "refresh_token",
        "refresh_token": MS_GRAPH_REFRESH_TOKEN,
        "scope":         "Mail.Read Files.Read.All offline_access",
    }, timeout=30)
    if not r.ok:
        print(f"  Token refresh failed: {r.status_code} {r.text[:300]}")
        return False
    data = r.json()
    ACCESS_TOKEN = data["access_token"]
    # Log new refresh token if returned (it lasts 90 days)
    new_rt = data.get("refresh_token")
    if new_rt:
        print("  New refresh token received (90-day lifetime). Update secret if needed.")
    print("  Access token acquired.")
    return True

def graph_headers():
    return {"Authorization": f"Bearer {ACCESS_TOKEN}", "Content-Type": "application/json"}

# ════════════════════════════════════════════════════════════════════════════
# GRAPH HELPERS — search emails, get attachments
# ════════════════════════════════════════════════════════════════════════════

def search_emails(search_query, top=5):
    """Search Outlook emails via Graph API. Returns list of message objects."""
    url = f'{GRAPH_BASE}/me/messages'
    params = {
        "$search": f'"{search_query}"',
        "$top": top,
        "$select": "id,subject,from,body,receivedDateTime,hasAttachments",
        "$orderby": "receivedDateTime desc",
    }
    r = requests.get(url, headers=graph_headers(), params=params, timeout=30)
    if not r.ok:
        print(f"  Search failed for '{search_query}': {r.status_code} {r.text[:200]}")
        return []
    return r.json().get("value", [])

def get_attachments(message_id):
    """Get attachments for a message. Returns list of attachment objects."""
    url = f'{GRAPH_BASE}/me/messages/{message_id}/attachments'
    r = requests.get(url, headers=graph_headers(), timeout=60)
    if not r.ok:
        print(f"  Get attachments failed: {r.status_code}")
        return []
    return r.json().get("value", [])

def get_body_text(msg):
    """Extract plain text from a message body."""
    body = msg.get("body", {})
    content = body.get("content", "")
    if body.get("contentType") == "html":
        # Strip HTML tags for parsing
        content = re.sub(r'<br\s*/?>', '\n', content, flags=re.IGNORECASE)
        content = re.sub(r'<[^>]+>', '', content)
        content = re.sub(r'&nbsp;', ' ', content)
        content = re.sub(r'&#\d+;', '', content)
    return content.strip()

# ════════════════════════════════════════════════════════════════════════════
# STEP 1: Fetch Cadiz Ops data via Graph API (Outlook search)
# ════════════════════════════════════════════════════════════════════════════

def fetch_cadiz_ops():
    """Search Outlook for switch times, LOGS, and carrier projections."""
    today = date.today()
    yesterday = today - timedelta(days=1)
    today_str = today.strftime('%m.%d.%y')
    yesterday_str = yesterday.strftime('%m.%d.%y')

    result = {
        "date": yesterday_str,
        "switch_start": "N/A",
        "switch_end": "N/A",
        "loaded_cars_out": 0,
        "empty_cars_in": 0,
        "maintenance_notes": [],
        "carrier_projections": {},
        "yesterday_bbls_from_logs": None,
        "yesterday_trucks_from_logs": None,
    }

    # --- RAIL SWAP email ---
    print("  Searching for RAIL SWAP email...")
    msgs = search_emails(f"from:cadiz.ops subject:RAIL")
    for msg in msgs:
        subj = msg.get("subject", "")
        if today_str in subj or yesterday_str in subj:
            body = get_body_text(msg)
            # Parse: START TIME 3:05 AM
            start_m = re.search(r'START\s+TIME\s+(\d{1,2}:\d{2}\s*[AP]M)', body, re.IGNORECASE)
            end_m = re.search(r'END\s+TIME\s+(\d{1,2}:\d{2}\s*[AP]M)', body, re.IGNORECASE)
            loaded_m = re.search(r'(\d+)\s+LOADED\s+CARS?\s+SENT', body, re.IGNORECASE)
            empty_m = re.search(r'(\d+)\s+EMPTY\s+CARS?\s+PUSHED', body, re.IGNORECASE)
            if start_m:
                result["switch_start"] = start_m.group(1).strip()
            if end_m:
                result["switch_end"] = end_m.group(1).strip()
            if loaded_m:
                result["loaded_cars_out"] = int(loaded_m.group(1))
            if empty_m:
                result["empty_cars_in"] = int(empty_m.group(1))
            print(f"    Found RAIL SWAP: {result['switch_start']} -> {result['switch_end']}")
            break

    # --- UPDATE email (switch end / resume time in subject) ---
    print("  Searching for UPDATE email...")
    msgs = search_emails(f"from:cadiz.ops subject:UPDATE")
    for msg in msgs:
        subj = msg.get("subject", "")
        if today_str in subj or yesterday_str in subj:
            # Subject: "03.25.26 4:48 AM UPDATE"
            time_m = re.search(r'(\d{1,2}:\d{2}\s*[AP]M)\s+UPDATE', subj, re.IGNORECASE)
            if time_m:
                result["switch_end"] = time_m.group(1).strip()
                print(f"    UPDATE resume time: {result['switch_end']}")
            body = get_body_text(msg)
            if body.strip():
                result["maintenance_notes"].append(body.strip()[:200])
            break

    # --- LOGS email (yesterday's BBLs and trucks) ---
    print("  Searching for LOGS email...")
    msgs = search_emails(f"from:cadiz.ops subject:LOGS")
    for msg in msgs:
        subj = msg.get("subject", "")
        if today_str in subj or yesterday_str in subj:
            body = get_body_text(msg)
            # Parse: "59 TRUCKS - 11,474.24 BBLS"
            logs_m = re.search(r'(\d+)\s+TRUCKS?\s*[-\u2013]\s*([\d,]+\.?\d*)\s+BBLS?', body, re.IGNORECASE)
            if logs_m:
                result["yesterday_trucks_from_logs"] = int(logs_m.group(1))
                result["yesterday_bbls_from_logs"] = float(logs_m.group(2).replace(',', ''))
                print(f"    LOGS: {result['yesterday_trucks_from_logs']} trucks, {result['yesterday_bbls_from_logs']} BBLs")
            break

    # --- Carrier projections ---
    carrier_searches = {
        'Badlands':     'from:ohiodispatch subject:UPDATE',
        'KAG':          'from:bxi-bloomingdale subject:UPDATE',
    }
    for cname, query in carrier_searches.items():
        print(f"  Searching for {cname} carrier reply...")
        msgs = search_emails(query)
        trucks = 0
        note = "No response"
        for msg in msgs:
            subj = msg.get("subject", "")
            recv = msg.get("receivedDateTime", "")
            # Only today's messages
            if today.strftime('%Y-%m-%d') in recv[:10]:
                body = get_body_text(msg)
                # Look for truck count: "We have 9 planned so far"
                truck_m = re.search(r'(\d+)\s+(?:planned|trucks?|loads?|scheduled)', body, re.IGNORECASE)
                if not truck_m:
                    truck_m = re.search(r'(?:have|running|sending|doing)\s+(\d+)', body, re.IGNORECASE)
                if truck_m:
                    trucks = int(truck_m.group(1))
                    note = ""
                    print(f"    {cname}: {trucks} trucks")
                break

        avg = CARRIER_AVGS.get(cname, 190)
        result["carrier_projections"][cname] = {
            "trucks": trucks,
            "proj_bbls": round(trucks * avg),
            "note": note,
        }

    # Fill in carriers that don't have dedicated searches
    for cname in ['Prop Logistics', 'BD Oil', '1st Choice Energy']:
        if cname not in result["carrier_projections"]:
            result["carrier_projections"][cname] = {
                "trucks": 0, "proj_bbls": 0, "note": "No response"
            }

    return result

# ════════════════════════════════════════════════════════════════════════════
# STEP 2: Fetch Master Load Log Excel from Rob's email attachment
# ════════════════════════════════════════════════════════════════════════════

def fetch_load_log_data(cadiz_data=None):
    """Find Rob's Master Load Log email, download Excel, parse with pandas."""
    yesterday = date.today() - timedelta(days=1)
    yesterday_str = yesterday.strftime('%m.%d.%y')

    print("  Searching for Master Load Log email from Rob...")
    msgs = search_emails("from:robk subject:Master Load Log")

    excel_bytes = None
    for msg in msgs:
        if not msg.get("hasAttachments"):
            continue
        # Get the most recent one
        attachments = get_attachments(msg["id"])
        for att in attachments:
            name = att.get("name", "")
            if name.lower().endswith(('.xlsx', '.xls')) and 'load log' in name.lower():
                content_bytes = att.get("contentBytes")
                if content_bytes:
                    excel_bytes = base64.b64decode(content_bytes)
                    print(f"    Found attachment: {name} ({len(excel_bytes):,} bytes)")
                    break
        if excel_bytes:
            break

    if not excel_bytes:
        print("  ERROR: Could not find Master Load Log Excel attachment")
        return None

    # Parse with pandas
    return parse_load_log(excel_bytes, cadiz_data)

def parse_load_log(excel_bytes, cadiz_data=None):
    """Parse the Master Load Log Excel file."""
    yesterday = date.today() - timedelta(days=1)
    yesterday_str = yesterday.strftime('%m.%d.%y')

    try:
        df = pd.read_excel(io.BytesIO(excel_bytes), sheet_name='Master_Load_Log')
    except Exception as e:
        print(f"  ERROR reading Excel sheet 'Master_Load_Log': {e}")
        # Try first sheet as fallback
        try:
            df = pd.read_excel(io.BytesIO(excel_bytes), sheet_name=0)
            print("  Falling back to first sheet")
        except Exception as e2:
            print(f"  ERROR reading any sheet: {e2}")
            return None

    print(f"  Loaded {len(df)} rows from Excel")

    # Normalize column names
    df.columns = [str(c).strip() for c in df.columns]

    # Find relevant columns
    date_col = None
    bol_col = None
    bbls_col = None
    pump_time_col = None
    split_col = None

    for c in df.columns:
        cl = c.lower()
        if 'date' in cl and date_col is None:
            date_col = c
        elif 'bol' in cl and bol_col is None:
            bol_col = c
        elif 'metered' in cl and 'bbl' in cl and bbls_col is None:
            bbls_col = c
        elif 'pump' in cl and 'time' in cl and pump_time_col is None:
            pump_time_col = c
        elif 'split' in cl and split_col is None:
            split_col = c

    print(f"  Columns mapped: date={date_col}, bol={bol_col}, bbls={bbls_col}, pump_time={pump_time_col}, split={split_col}")

    if not all([date_col, bol_col, bbls_col]):
        print("  ERROR: Could not find required columns")
        return None

    # Filter to March 2026 data
    df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
    march_start = datetime(2026, 3, 1)
    march_end = datetime(2026, 3, 31, 23, 59, 59)
    df_march = df[(df[date_col] >= march_start) & (df[date_col] <= march_end)].copy()
    print(f"  March 2026 rows: {len(df_march)}")

    if len(df_march) == 0:
        print("  ERROR: No March 2026 data found")
        return None

    # Determine pump from BOL prefix
    def get_pump(bol):
        s = str(bol).strip()
        if s.startswith('111'):
            return 'P-101'
        elif s.startswith('222'):
            return 'P-102'
        elif s.startswith('333'):
            return 'P-103'
        return None

    df_march['pump'] = df_march[bol_col].apply(get_pump)

    # Identify split loads
    def is_split2(val):
        if pd.isna(val):
            return False
        return 'split #2' in str(val).lower() or 'split#2' in str(val).lower().replace(' ', '')

    if split_col:
        df_march['is_split2'] = df_march[split_col].apply(is_split2)
    else:
        df_march['is_split2'] = False

    # Parse pump time to hours
    def parse_pump_time(val):
        if pd.isna(val):
            return 0.0
        s = str(val).strip()
        m = re.match(r'(\d+):(\d{2})', s)
        if m:
            return int(m.group(1)) + int(m.group(2)) / 60.0
        try:
            return float(s)
        except:
            return 0.0

    if pump_time_col:
        df_march['pump_hrs'] = df_march[pump_time_col].apply(parse_pump_time)
    else:
        df_march['pump_hrs'] = 0.0

    df_march['bbls_val'] = pd.to_numeric(df_march[bbls_col], errors='coerce').fillna(0)

    # --- Yesterday's data ---
    yesterday_dt = pd.Timestamp(yesterday)
    df_yday = df_march[df_march[date_col].dt.date == yesterday]
    yday_bbls = df_yday['bbls_val'].sum()
    yday_trucks = len(df_yday[~df_yday['is_split2']])

    # Use LOGS email data if available and Excel data seems off
    if cadiz_data and cadiz_data.get('yesterday_bbls_from_logs'):
        logs_bbls = cadiz_data['yesterday_bbls_from_logs']
        logs_trucks = cadiz_data['yesterday_trucks_from_logs']
        if yday_bbls == 0 and logs_bbls > 0:
            print(f"  Using LOGS email data for yesterday: {logs_bbls} BBLs, {logs_trucks} trucks")
            yday_bbls = logs_bbls
            yday_trucks = logs_trucks

    # --- Pump utilization for yesterday ---
    pumps = {}
    for pname in ['P-101', 'P-102', 'P-103']:
        df_pump = df_yday[df_yday['pump'] == pname]
        loads = len(df_pump)
        splits = len(df_pump[df_pump['is_split2']])
        runtime = df_pump['pump_hrs'].sum()
        bbls = df_pump['bbls_val'].sum()
        ute = round(runtime / 21.0 * 100, 1) if runtime > 0 else 0
        bhr = round(bbls / runtime) if runtime > 0 else 0
        pumps[pname] = {
            'loads': loads,
            'splits': splits,
            'runtime_hrs': round(runtime, 2),
            'ute_pct': ute,
            'bbls': round(bbls),
            'bbls_hr': bhr,
        }

    # --- MTD data ---
    unique_dates = sorted(df_march[date_col].dt.date.unique())
    mtd_days = len(unique_dates)
    mtd_total_bbls = df_march['bbls_val'].sum()
    mtd_total_trucks = len(df_march[~df_march['is_split2']])
    avg_bbls = round(mtd_total_bbls / mtd_days, 1) if mtd_days > 0 else 0

    # --- Daily data for weekly table ---
    daily_data = []
    for d in unique_dates:
        df_day = df_march[df_march[date_col].dt.date == d]
        d_bbls = df_day['bbls_val'].sum()
        d_trucks = len(df_day[~df_day['is_split2']])
        daily_data.append({
            "date": d.strftime('%Y-%m-%d'),
            "bbls": round(d_bbls, 2),
            "trucks": d_trucks,
        })

    result = {
        "yesterday_date": yesterday_str,
        "yesterday_bbls": round(yday_bbls, 2),
        "yesterday_trucks": yday_trucks,
        "mtd_days": mtd_days,
        "mtd_total_bbls": round(mtd_total_bbls, 2),
        "mtd_total_trucks": mtd_total_trucks,
        "avg_bbls_per_day": avg_bbls,
        "pump_utilization": pumps,
        "daily_data": daily_data,
    }

    print(f"  Yesterday: {yday_bbls:,.2f} BBLs, {yday_trucks} trucks")
    print(f"  MTD: {mtd_days} days, {mtd_total_bbls:,.2f} BBLs, avg {avg_bbls:,.1f}/day")
    return result

# ════════════════════════════════════════════════════════════════════════════
# STEP 3: Build HTML email
# ════════════════════════════════════════════════════════════════════════════

def build_briefing_html(load_data, cadiz_data):
    """Build the full HTML briefing email."""
    today = date.today()
    yesterday = today - timedelta(days=1)
    day_name = today.strftime('%A')
    date_long = today.strftime('%B %d, %Y')

    # Extract load log data
    yday_bbls = load_data.get('yesterday_bbls', 0)
    yday_trucks = load_data.get('yesterday_trucks', 0)
    yday_bpt = round(yday_bbls / yday_trucks, 1) if yday_trucks > 0 else 0
    mtd_days = load_data.get('mtd_days', 1)
    mtd_bbls = load_data.get('mtd_total_bbls', 0)
    mtd_trucks = load_data.get('mtd_total_trucks', 0)
    avg_bbls = load_data.get('avg_bbls_per_day', 0)
    days_remain = MARCH_DAYS - mtd_days
    proj_bbls = mtd_bbls + avg_bbls * days_remain
    proj_trucks = mtd_trucks + (mtd_trucks / mtd_days * days_remain) if mtd_days > 0 else 0
    rail_cap = avg_bbls / RAIL_CAP_DAILY * 100 if RAIL_CAP_DAILY > 0 else 0
    vs_rate = ((yday_bbls - avg_bbls) / avg_bbls * 100) if avg_bbls > 0 else 0
    vs_feb = ((avg_bbls - FEB_AVG_DAILY) / FEB_AVG_DAILY * 100)

    # Pump utilization
    pumps = load_data.get('pump_utilization', {})
    pump_rows = ""
    total_rt = 0
    total_loads = 0
    total_splits = 0
    total_bbls_pumps = 0
    for pname in ['P-101', 'P-102', 'P-103']:
        p = pumps.get(pname, {})
        loads = p.get('loads', 0)
        splits = p.get('splits', 0)
        rt = p.get('runtime_hrs', 0)
        ute = p.get('ute_pct', 0)
        bbls = p.get('bbls', 0)
        bhr = p.get('bbls_hr', 0)
        total_rt += rt
        total_loads += loads
        total_splits += splits
        total_bbls_pumps += bbls
        pump_rows += f"<tr><td>{pname}</td><td>{loads}</td><td>{splits}</td><td>{rt} hrs</td><td>{ute}%</td><td>{bbls:,.0f}</td><td>{bhr:.0f}</td></tr>"

    combined_ute = round(total_rt / (21 * 3) * 100, 1)
    total_bhr = round(total_bbls_pumps / total_rt, 0) if total_rt > 0 else 0
    pump_rows += f"<tr style='font-weight:bold;border-top:2px solid #333'><td>Combined</td><td>{total_loads}</td><td>{total_splits}</td><td>{total_rt:.2f} hrs</td><td>{combined_ute}%</td><td>{total_bbls_pumps:,.0f}</td><td>{total_bhr:.0f}</td></tr>"

    # Cadiz ops activity
    switch_start = cadiz_data.get('switch_start', 'N/A') if cadiz_data else 'N/A'
    switch_end = cadiz_data.get('switch_end', 'N/A') if cadiz_data else 'N/A'
    carriers = cadiz_data.get('carrier_projections', {}) if cadiz_data else {}

    carrier_rows = ""
    total_carrier_trucks = 0
    total_carrier_bbls = 0
    for cname in ['Badlands', 'KAG', 'Prop Logistics', 'BD Oil', '1st Choice Energy']:
        c = carriers.get(cname, {})
        trucks = c.get('trucks', 0)
        note = c.get('note', '')
        if trucks > 0:
            avg = CARRIER_AVGS.get(cname, 190)
            proj = round(trucks * avg)
            total_carrier_trucks += trucks
            total_carrier_bbls += proj
            carrier_rows += f"<tr><td>{cname}</td><td>{trucks}</td><td>{avg}</td><td>{proj:,}</td></tr>"
        else:
            carrier_rows += f"<tr><td>{cname}</td><td colspan='3' style='color:#999'>No response</td></tr>"

    carrier_rows += f"<tr style='font-weight:bold;border-top:2px solid #333'><td>Total</td><td>{total_carrier_trucks}</td><td>&mdash;</td><td>~{total_carrier_bbls:,} BBLs</td></tr>"

    # Weekly data
    daily_data = load_data.get('daily_data', [])
    current_week_start = yesterday - timedelta(days=yesterday.weekday() + 2)  # Saturday
    if current_week_start.weekday() != 5:
        current_week_start -= timedelta(days=(current_week_start.weekday() + 2) % 7)

    week_rows = ""
    week_total_bbls = 0
    week_total_trucks = 0
    week_days = 0
    for dd in daily_data:
        try:
            d_date = datetime.strptime(dd['date'], '%Y-%m-%d').date()
            if d_date >= current_week_start and d_date <= yesterday:
                day_abbr = d_date.strftime('%b %d (%a)')
                d_bbls = dd.get('bbls', 0)
                d_trucks = dd.get('trucks', 0)
                d_bpt = round(d_bbls / d_trucks, 1) if d_trucks > 0 else 0
                vs = ((d_bbls - avg_bbls) / avg_bbls * 100) if avg_bbls > 0 else 0
                col = '#4caf50' if vs >= 0 else '#ef5350'
                sign = '+' if vs >= 0 else ''
                week_rows += f"<tr><td>{day_abbr}</td><td>{d_bbls:,.0f}</td><td>{d_trucks}</td><td>{d_bpt:.1f}</td><td style='color:{col}'>{sign}{vs:.1f}%</td></tr>"
                week_total_bbls += d_bbls
                week_total_trucks += d_trucks
                week_days += 1
        except:
            continue

    if week_days > 0:
        week_avg_bpt = round(week_total_bbls / week_total_trucks, 1) if week_total_trucks > 0 else 0
        week_vs = ((week_total_bbls / week_days - avg_bbls) / avg_bbls * 100) if avg_bbls > 0 else 0
        col = '#4caf50' if week_vs >= 0 else '#ef5350'
        sign = '+' if week_vs >= 0 else ''
        week_rows += f"<tr style='font-weight:bold;border-top:2px solid #333'><td>{week_days}-Day Total</td><td>{week_total_bbls:,.0f}</td><td>{week_total_trucks}</td><td>{week_avg_bpt}</td><td style='color:{col}'>{sign}{week_vs:.1f}% avg</td></tr>"

    # Flags
    flags = []
    for pname in ['P-101', 'P-102', 'P-103']:
        p = pumps.get(pname, {})
        if p.get('ute_pct', 0) < 30:
            others = [pumps.get(pn, {}).get('ute_pct', 0) for pn in ['P-101', 'P-102', 'P-103'] if pn != pname]
            max_other = max(others) if others else 0
            if max_other > 40:
                flags.append(f"{pname} ute {p['ute_pct']}% vs {max_other}% &mdash; load imbalance across pumps.")
        if p.get('bbls_hr', 0) > 0 and p.get('bbls_hr', 999) < 438:
            flags.append(f"BBLs/hr below Feb avg (438): {pname} at {p['bbls_hr']:.0f}. Consistent with gap time between trucks.")

    split_pct = round(total_splits / total_loads * 100, 1) if total_loads > 0 else 0
    flags.append(f"Split count {total_splits}/{total_loads} ({split_pct}%) &mdash; {'near' if abs(split_pct - 24) < 5 else 'above' if split_pct > 24 else 'below'} historical avg (24%).")

    flags_html = "".join(f"<p style='margin:4px 0'>{f}</p>" for f in flags)

    vs_col = '#4caf50' if vs_rate >= 0 else '#ef5350'
    vs_sign = '+' if vs_rate >= 0 else ''

    html = f"""<!DOCTYPE html>
<html><head><meta charset="utf-8"><style>
body {{ font-family: -apple-system, 'Segoe UI', Roboto, sans-serif; max-width: 820px; margin: 0 auto; padding: 20px; color: #1a1a1a; background: #f8f9fa; }}
h1 {{ font-size: 22px; border-bottom: 3px solid #1a5276; padding-bottom: 8px; }}
h2 {{ font-size: 16px; color: #1a5276; margin-top: 24px; border-bottom: 1px solid #ddd; padding-bottom: 4px; }}
.kpi-grid {{ display: grid; grid-template-columns: repeat(4, 1fr); gap: 12px; margin: 12px 0; }}
.kpi {{ background: #fff; border: 1px solid #e0e0e0; border-radius: 6px; padding: 12px; text-align: center; }}
.kpi-val {{ font-size: 22px; font-weight: bold; color: #1a5276; }}
.kpi-label {{ font-size: 11px; color: #666; margin-top: 4px; }}
table {{ width: 100%; border-collapse: collapse; font-size: 13px; background: #fff; margin: 8px 0; }}
th {{ background: #1a5276; color: white; padding: 6px 10px; text-align: left; font-weight: 500; }}
td {{ padding: 5px 10px; border-bottom: 1px solid #eee; }}
tr:nth-child(even) {{ background: #f8f9fa; }}
.flags {{ background: #fff8e1; border-left: 4px solid #ff9800; padding: 10px 14px; margin: 12px 0; font-size: 13px; }}
.footer {{ font-size: 11px; color: #999; margin-top: 30px; padding-top: 10px; border-top: 1px solid #ddd; }}
</style></head><body>

<h1>&#x1F4CA; Timiron Daily Briefing | Cadiz Terminal</h1>
<p style="color:#666;margin-top:-8px">{day_name}, {date_long} &middot; Based on {yesterday.strftime('%B %d, %Y')} LOGS</p>

<h2>&#x1F4C5; Yesterday &mdash; {yesterday.strftime('%B %d, %Y')}</h2>
<div class="kpi-grid">
  <div class="kpi"><div class="kpi-val">{yday_bbls:,.2f}</div><div class="kpi-label">BBLs</div></div>
  <div class="kpi"><div class="kpi-val">{yday_trucks}</div><div class="kpi-label">Trucks</div></div>
  <div class="kpi"><div class="kpi-val">{yday_bpt}</div><div class="kpi-label">Avg BBLs / Truck</div></div>
  <div class="kpi"><div class="kpi-val" style="color:{vs_col}">{vs_sign}{vs_rate:.1f}%</div><div class="kpi-label">vs Run Rate Avg</div></div>
</div>

<h2>&#x2699;&#xFE0F; Pump Utilization &mdash; {yesterday.strftime('%B %d, %Y')}</h2>
<table>
<tr><th>Pump</th><th>Loads</th><th>Splits</th><th>Runtime</th><th>Ute %</th><th>BBLs</th><th>BBLs/Hr</th></tr>
{pump_rows}
</table>
<p style="font-size:11px;color:#888">Ute % = runtime / 21 true available hrs/pump (24hr - 3hr rail switch) &middot; Feb 2026 avg: {FEB_AVG_UTE}%</p>

<h2>&#x1F4C6; Month-to-Date (Mar 1&ndash;{yesterday.day})</h2>
<div class="kpi-grid">
  <div class="kpi"><div class="kpi-val">{mtd_bbls:,.0f}</div><div class="kpi-label">MTD BBLs</div></div>
  <div class="kpi"><div class="kpi-val">{avg_bbls:,.1f}</div><div class="kpi-label">Daily Avg ({mtd_days} days)</div></div>
  <div class="kpi"><div class="kpi-val" style="color:{'#4caf50' if vs_feb >= 0 else '#ef5350'}">{'+' if vs_feb >= 0 else ''}{vs_feb:.1f}%</div><div class="kpi-label">vs Feb Avg ({FEB_AVG_DAILY:,}/day)</div></div>
  <div class="kpi"><div class="kpi-val">{rail_cap:.1f}%</div><div class="kpi-label">% of Rail Cap (15k/day)</div></div>
</div>

<h2>&#x1F4C8; March Forecast ({mtd_days}-Day Run Rate)</h2>
<div class="kpi-grid">
  <div class="kpi"><div class="kpi-val">{avg_bbls:,.1f}</div><div class="kpi-label">Run Rate Avg bbls/day</div></div>
  <div class="kpi"><div class="kpi-val">{proj_bbls:,.0f}</div><div class="kpi-label">Projected Total BBLs</div></div>
  <div class="kpi"><div class="kpi-val">~{proj_trucks:,.0f}</div><div class="kpi-label">Projected Trucks</div></div>
  <div class="kpi"><div class="kpi-val" style="color:{'#4caf50' if proj_bbls > FEB_TOTAL_BBLS else '#ef5350'}">{'+' if proj_bbls > FEB_TOTAL_BBLS else ''}{proj_bbls - FEB_TOTAL_BBLS:,.0f}</div><div class="kpi-label">vs Feb ({FEB_TOTAL_BBLS:,} BBLs)</div></div>
</div>

<h2>&#x1F4CA; Current Week</h2>
<table>
<tr><th>Date</th><th>BBLs</th><th>Trucks</th><th>BBLs/Truck</th><th>vs Run Rate</th></tr>
{week_rows}
</table>

<h2>&#x1F4E1; Cadiz Ops Activity</h2>
<div class="kpi-grid" style="grid-template-columns: 1fr;">
  <div class="kpi"><div class="kpi-val">Rail Switch {switch_start} &rarr; {switch_end}</div></div>
</div>

<h2>&#x1F69B; Carrier Projections</h2>
<table>
<tr><th>Carrier</th><th>Trucks</th><th>Avg BBLs/Truck</th><th>Proj BBLs</th></tr>
{carrier_rows}
</table>

<div class="flags">
<strong>&#x26A0;&#xFE0F; Flags</strong>
{flags_html}
</div>

<div class="footer">
Timiron Midstream Partners &middot; Cadiz Terminal, OH &middot; Auto-generated via GitHub Actions<br>
Sent from {GMAIL_ADDRESS}
</div>

</body></html>"""
    return html

# ════════════════════════════════════════════════════════════════════════════
# STEP 4: Send email via Gmail SMTP
# ════════════════════════════════════════════════════════════════════════════

def send_email(html_body):
    today = date.today()
    day_name = today.strftime('%A')
    date_long = today.strftime('%B %d, %Y')
    subject = f"\U0001F4CA Timiron Daily Briefing | {day_name}, {date_long}"

    msg = MIMEMultipart('alternative')
    msg['Subject'] = subject
    msg['From'] = GMAIL_ADDRESS
    msg['To'] = ', '.join(RECIPIENTS)
    msg.attach(MIMEText(html_body, 'html'))

    with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
        smtp.login(GMAIL_ADDRESS, GMAIL_APP_PASS)
        smtp.send_message(msg)
    print(f"  Email sent to: {', '.join(RECIPIENTS)}")

# ════════════════════════════════════════════════════════════════════════════
# MAIN
# ════════════════════════════════════════════════════════════════════════════

def main():
    today = date.today()
    print("=" * 60)
    print(f"  Timiron Cloud Briefing -- {today.strftime('%B %d, %Y')}")
    print("=" * 60)

    # Validate config
    if not MS_GRAPH_REFRESH_TOKEN:
        print("ERROR: MS_GRAPH_REFRESH_TOKEN not set"); sys.exit(1)
    if not MS_GRAPH_CLIENT_ID:
        print("ERROR: MS_GRAPH_CLIENT_ID not set"); sys.exit(1)
    if not GMAIL_APP_PASS:
        print("ERROR: GMAIL_APP_PASS not set"); sys.exit(1)

    # Step 0: Get access token
    print("\n[0] Authenticating with Microsoft Graph...")
    if not get_access_token():
        print("  ERROR: Could not authenticate with Microsoft Graph")
        sys.exit(1)

    # Step 1: Fetch Cadiz Ops data from Outlook
    print("\n[1] Fetching Cadiz Ops data from Outlook...")
    cadiz_data = fetch_cadiz_ops()
    if cadiz_data:
        print("  OK")
    else:
        print("  Warning: No Cadiz ops data - will show N/A in briefing")

    # Step 2: Fetch Master Load Log data from email attachment
    print("\n[2] Fetching Master Load Log from email attachment...")
    load_data = fetch_load_log_data(cadiz_data)
    if not load_data:
        print("  ERROR: Could not read load log data")
        sys.exit(1)
    print("  OK")

    # Step 3: Build HTML email
    print("\n[3] Building briefing email...")
    html = build_briefing_html(load_data, cadiz_data)
    print(f"  HTML size: {len(html):,} bytes")

    # Step 4: Send email
    print("\n[4] Sending email...")
    send_email(html)

    print("\n" + "=" * 60)
    print("  Done.")
    print("=" * 60)

if __name__ == "__main__":
    main()
