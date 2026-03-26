"""
timiron_cloud_briefing.py — Cloud version of Timiron Daily Briefing
Runs on GitHub Actions at 6 AM ET daily.
Uses Microsoft Graph API directly for Outlook email search and attachments.
Sends dark-themed HTML briefing email via Gmail SMTP with TWO Excel attachments.

Output is identical to the local script (timiron_daily_update.py).
"""

import os, json, re, sys, shutil, smtplib, base64, io, tempfile
from datetime import date, timedelta, datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import requests
import pandas as pd
import openpyxl

# ════════════════════════════════════════════════════════════════════════════
# CONFIG — from GitHub Secrets (environment variables)
# ════════════════════════════════════════════════════════════════════════════

MS_GRAPH_REFRESH_TOKEN = os.environ.get('MS_GRAPH_REFRESH_TOKEN', '')
MS_GRAPH_CLIENT_ID     = os.environ.get('MS_GRAPH_CLIENT_ID', '')
GMAIL_ADDRESS          = os.environ.get('GMAIL_ADDRESS', 'tyk216@gmail.com')
GMAIL_APP_PASS         = os.environ.get('GMAIL_APP_PASS', '')
RECIPIENTS             = os.environ.get('RECIPIENTS', 'tylerk@timironmp.com,robk@timirontrading.com').split(',')

# Templates live in ./templates/ directory in the repo
SCRIPT_DIR   = os.path.dirname(os.path.abspath(__file__))
TEMPLATE_DIR = os.path.join(SCRIPT_DIR, "templates")

CARRIER_AVGS = {
    'Badlands':          224.6,
    'KAG':               188.9,
    'Prop Logistics':    181.5,
    'BD Oil':            188.2,
    '1st Choice Energy': 183.3,
}

# ════════════════════════════════════════════════════════════════════════════
# CONSTANTS — copied from local script
# ════════════════════════════════════════════════════════════════════════════

MARCH_DAYS        = 31
RAIL_CAP_DAILY    = 15000
MARCH_FIXED_COST  = 244583.52
PUMP_HRS_MONTH    = 744
YTD_BBLS_PRE_MAR   = 2520947 - 329633
YTD_TRUCKS_PRE_MAR = 13218 - 2039

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
# FETCH CADIZ OPS DATA — via Graph API (Outlook search)
# ════════════════════════════════════════════════════════════════════════════

def fetch_cadiz_ops():
    """Search Outlook for switch times, LOGS, and carrier projections."""
    today = date.today()
    yesterday = today - timedelta(days=1)
    today_str = today.strftime('%m.%d.%y')
    yesterday_str = yesterday.strftime('%m.%d.%y')

    result = {
        "date": yesterday_str,
        "switch_start": None,
        "switch_end": None,
        "loaded_cars_out": 0,
        "empty_cars_in": 0,
        "maintenance_notes": [],
        "carrier_projections": {},
    }

    # --- RAIL SWAP email ---
    print("  Searching for RAIL SWAP email...")
    msgs = search_emails(f"from:cadiz.ops subject:RAIL")
    for msg in msgs:
        subj = msg.get("subject", "")
        if today_str in subj or yesterday_str in subj:
            body = get_body_text(msg)
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
            time_m = re.search(r'(\d{1,2}:\d{2}\s*[AP]M)\s+UPDATE', subj, re.IGNORECASE)
            if time_m:
                result["switch_end"] = time_m.group(1).strip()
                print(f"    UPDATE resume time: {result['switch_end']}")
            body = get_body_text(msg)
            # Only capture actual maintenance keywords, not entire email body
            maint_keywords = ['pump', 'repair', 'replace', 'fix', 'broke', 'leak', 'down', 'out of service',
                              'maintenance', 'welding', 'valve', 'hose', 'motor', 'pressure', 'gauge']
            if body.strip():
                lines = body.strip().split('\n')
                for line in lines:
                    line = line.strip()
                    if line and len(line) > 10 and any(kw in line.lower() for kw in maint_keywords):
                        result["maintenance_notes"].append(line[:200])
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
            if today.strftime('%Y-%m-%d') in recv[:10]:
                body = get_body_text(msg)
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
# FETCH MASTER LOAD LOG — download Excel from cadiz.ops LOGS email attachment
# ════════════════════════════════════════════════════════════════════════════

def fetch_load_log_excel():
    """Find cadiz.ops LOGS email with Master Load Log attachment.
    Returns (excel_bytes, excel_filename) or (None, None).
    """
    print("  Searching for LOGS email from cadiz.ops...")
    msgs = search_emails("from:cadiz.ops subject:LOGS")

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
                    print(f"    Found attachment: {name} ({len(excel_bytes):,} bytes)")
                    return excel_bytes, name
    print("  ERROR: Could not find Master Load Log Excel attachment")
    return None, None

# ════════════════════════════════════════════════════════════════════════════
# PARSE LOAD LOG — copied from local script (identical logic)
# ════════════════════════════════════════════════════════════════════════════

def parse_load_log(excel_bytes):
    """Parse the Master Load Log Excel from bytes. Returns dict matching local script output."""
    df = pd.read_excel(io.BytesIO(excel_bytes), sheet_name='Master_Load_Log', header=0)
    df['Date']       = pd.to_datetime(df['Date']).dt.date
    df['BOL_prefix'] = df['Timiron BOL#'].astype(str).str[:3]
    df['Metered']    = pd.to_numeric(df['Timiron Metered bbls.'], errors='coerce').fillna(0)

    def to_mins(t):
        try: s=str(t); p=s.split(':'); return int(p[0])*60+int(p[1])
        except: return 0

    df['pump_mins'] = df['Pump Time'].apply(to_mins)
    march = df[df['Date'] >= date(2026,3,1)].copy()
    if march.empty: raise ValueError("No March 2026 data in load log.")

    # Use yesterday's date (load log is always dated yesterday)
    yesterday_date = date.today() - timedelta(days=1)

    # MTD = all days up to and including yesterday
    mtd_data = march[march['Date'] <= yesterday_date]

    yday_count = len(march[march['Date'] == yesterday_date])
    print(f"  Yesterday: {yesterday_date}  ({yday_count} loads)")
    mtd_days = sorted(mtd_data['Date'].unique())
    print(f"  MTD: {len(mtd_days)} days  ({min(mtd_days)} -- {max(mtd_days)})")

    yday = march[march['Date'] == yesterday_date]
    pump_map = {'111':'P-101','222':'P-102','333':'P-103'}
    pump_ute = {}
    for prefix, pname in pump_map.items():
        p         = yday[yday['BOL_prefix']==prefix]
        splits    = p[p['Split Load'].astype(str).str.contains('Split #2',na=False)]
        non_split = p[~p['Split Load'].astype(str).str.contains('Split #2',na=False)]
        runtime   = p['pump_mins'].sum()/60
        bbls      = p['Metered'].sum()
        pump_ute[pname] = {
            'loads':   len(non_split), 'splits': len(splits),
            'runtime': round(runtime,2), 'ute': round(runtime/21*100,1),
            'bbls':    round(bbls,2),
            'bbl_hr':  round(bbls/runtime,0) if runtime>0 else 0
        }
    combined_rt = sum(v['runtime'] for v in pump_ute.values())

    mtd_no_split = mtd_data[~mtd_data['Split Load'].astype(str).str.contains('Split #2',na=False)]
    daily_trucks = mtd_no_split.groupby('Date').size()
    daily_bbls   = mtd_data.groupby('Date')['Metered'].sum()
    total_bbls   = daily_bbls.sum()
    total_trucks = daily_trucks.sum()
    days_actual  = len(daily_bbls)
    days_remain  = MARCH_DAYS - days_actual
    avg_bbls     = total_bbls / days_actual
    avg_trucks   = total_trucks / days_actual
    proj_bbls    = total_bbls + avg_bbls * days_remain
    proj_trucks  = total_trucks + avg_trucks * days_remain
    proj_rev     = rev_per_day(avg_bbls) * MARCH_DAYS
    ebitda       = proj_rev - MARCH_FIXED_COST
    rail_cap     = avg_bbls / RAIL_CAP_DAILY
    p101 = mtd_data[mtd_data['BOL_prefix']=='111']['pump_mins'].sum()/60
    p102 = mtd_data[mtd_data['BOL_prefix']=='222']['pump_mins'].sum()/60
    p103 = mtd_data[mtd_data['BOL_prefix']=='333']['pump_mins'].sum()/60

    print(f"  MTD BBLs:  {total_bbls:,.2f}  avg {avg_bbls:,.1f}/day")
    print(f"  Projected: {proj_bbls:,.0f} BBLs | {proj_trucks:,.0f} trucks")
    print(f"  Pump Ute:  P-101 {pump_ute['P-101']['ute']}%  P-102 {pump_ute['P-102']['ute']}%  P-103 {pump_ute['P-103']['ute']}%  Combined {combined_rt/(21*3)*100:.1f}%")

    # Carrier actuals from yesterday's load log
    carrier_name_map = {'BD OIL': 'BD Oil'}  # normalize casing
    carrier_actuals = {}
    if 'Carrier' in march.columns:
        yday_carriers = yday.copy()
        # Exclude Split #2 rows for truck count
        yday_no_split = yday_carriers[~yday_carriers['Split Load'].astype(str).str.contains('Split #2', na=False)]
        for carrier_name, grp in yday_no_split.groupby('Carrier'):
            normalized = carrier_name_map.get(carrier_name, carrier_name)
            actual_trucks = len(grp)
            actual_bbls = round(yday_carriers[yday_carriers['Carrier'] == carrier_name]['Metered'].sum(), 1)
            carrier_actuals[normalized] = {
                'trucks': actual_trucks,
                'bbls': actual_bbls,
            }
        actuals_str = ', '.join(f'{k}={v["trucks"]}' for k,v in carrier_actuals.items())
        print(f"  Carrier actuals: {actuals_str}")

    return dict(
        yesterday_date=yesterday_date, days_actual=days_actual, days_remain=days_remain,
        total_bbls=round(total_bbls,2), total_trucks=int(total_trucks),
        avg_bbls=round(avg_bbls,1), avg_trucks=round(avg_trucks,1),
        proj_bbls=round(proj_bbls,0), proj_trucks=round(proj_trucks,0),
        proj_rev=round(proj_rev,2), ebitda=round(ebitda,2),
        rail_cap=round(rail_cap,6), pump_ute=pump_ute,
        pump_ute_combined=round(combined_rt/(21*3),3),
        p101_hrs=round(p101,2), p102_hrs=round(p102,2), p103_hrs=round(p103,2),
        carrier_actuals=carrier_actuals,
    )

# ════════════════════════════════════════════════════════════════════════════
# UPDATE EXCEL FILES — copied from local script (identical logic)
# ════════════════════════════════════════════════════════════════════════════

def safe_write(ws, row, col, val):
    cell = ws.cell(row, col)
    if cell.__class__.__name__ != 'MergedCell':
        cell.value = val

def find_template(name_fragment):
    """Find template in ./templates/ directory."""
    exact = os.path.join(TEMPLATE_DIR, f"Timiron_{name_fragment}.xlsx")
    if os.path.exists(exact):
        print(f"  Template: Timiron_{name_fragment}.xlsx")
        return exact
    raise FileNotFoundError(
        f"\nTemplate not found: Timiron_{name_fragment}.xlsx"
        f"\nLooked in: {TEMPLATE_DIR}"
    )

def update_dashboard(template_path, d, output_path):
    shutil.copy(template_path, output_path)
    wb = openpyxl.load_workbook(output_path)
    ws = wb['Operations Dashboard']
    dt = d['yesterday_date'].strftime('%b %#d')
    r  = 16
    ws.cell(r, 3).value = d['proj_rev'];      ws.cell(r, 5).value = d['ebitda']
    ws.cell(r, 6).value = d['ebitda'];        ws.cell(r, 7).value = d['proj_bbls']
    ws.cell(r, 8).value = d['avg_bbls'];      ws.cell(r, 9).value = d['avg_trucks']
    ws.cell(r,10).value = d['proj_trucks'];   ws.cell(r,11).value = round(d['proj_rev']/d['proj_bbls'],2)
    ws.cell(r,12).value = round(MARCH_FIXED_COST/d['proj_bbls'],3)
    ws.cell(r,13).value = round(d['ebitda']/d['proj_bbls'],3)
    ws.cell(r,14).value = d['pump_ute_combined']; ws.cell(r,15).value = d['rail_cap']
    ws.cell(3,1).value  = (f"Source: Actual P&Ls, Master Load Logs, Payroll Files, Trafigura Invoices  |  "
                           f"Forecast: Mar 2026 based on {d['days_actual']}-day actuals (through {dt})  |  "
                           f"Mar 2026 costs from Mar P&L Forecast tab")
    ws.cell(18,1).value = (f"\u2020 Mar 2026 = FORECAST based on {d['days_actual']}-day actuals "
                           f"({d['avg_bbls']:.1f} bbls/day avg) + steady run-rate through Mar 31. "
                           f"Actuals through {dt} from daily Cadiz Ops LOGS emails. "
                           "Dec 2025 cost is negative due to 71k payroll reversal.\n\n"
                           "*  Adj Pump Util % = pump runtime hours \u00f7 true available hours (21 hrs/day).\n\n"
                           "**  % of Rail Cap/Day = avg bbls/day \u00f7 15,000 bbl daily rail ceiling.")
    pr = wb['Pump Runtime']
    for r2 in range(1,20):
        if pr.cell(r2,1).value and 'Mar 2026' in str(pr.cell(r2,1).value):
            pr.cell(r2, 3).value=d['p101_hrs']; pr.cell(r2, 4).value=round(PUMP_HRS_MONTH-d['p101_hrs'],2); pr.cell(r2, 5).value=round(d['p101_hrs']/PUMP_HRS_MONTH,3)
            pr.cell(r2, 6).value=d['p102_hrs']; pr.cell(r2, 7).value=round(PUMP_HRS_MONTH-d['p102_hrs'],2); pr.cell(r2, 8).value=round(d['p102_hrs']/PUMP_HRS_MONTH,3)
            pr.cell(r2, 9).value=d['p103_hrs']; pr.cell(r2,10).value=round(PUMP_HRS_MONTH-d['p103_hrs'],2); pr.cell(r2,11).value=round(d['p103_hrs']/PUMP_HRS_MONTH,3)
            pr.cell(r2,12).value=f"All 3 pumps active \u00b7 Partial month thru {dt}"; break
    wb.save(output_path)
    print(f"  Dashboard saved: {os.path.basename(output_path)}")

def update_external_report(template_path, d, output_path):
    shutil.copy(template_path, output_path)
    wb  = openpyxl.load_workbook(output_path)
    bbt = round(d['avg_bbls']/d['avg_trucks'],1) if d['avg_trucks']>0 else 0
    yb  = YTD_BBLS_PRE_MAR   + d['proj_bbls']
    yt  = YTD_TRUCKS_PRE_MAR + d['proj_trucks']
    dt  = d['yesterday_date'].strftime('%b %#d')
    to  = wb['Terminal Overview']
    safe_write(to,21, 3,d['proj_bbls']);      safe_write(to,21, 4,round(d['avg_bbls'],0))
    safe_write(to,21, 5,d['proj_trucks']);    safe_write(to,21, 6,d['avg_trucks'])
    safe_write(to,21, 7,bbt);                safe_write(to,21, 8,d['pump_ute_combined'])
    safe_write(to,21, 9,d['rail_cap']);       safe_write(to,21,10,round(1-d['rail_cap'],3))
    safe_write(to, 5, 1,round(yb,0));        safe_write(to, 5, 3,round(yt,0))
    om  = wb['Operational Metrics']
    safe_write(om,15, 3,d['p101_hrs']);  safe_write(om,15, 4,round(d['p101_hrs']/PUMP_HRS_MONTH,3))
    safe_write(om,15, 5,d['p102_hrs']);  safe_write(om,15, 6,round(d['p102_hrs']/PUMP_HRS_MONTH,3))
    safe_write(om,15, 7,d['p103_hrs']);  safe_write(om,15, 8,round(d['p103_hrs']/PUMP_HRS_MONTH,3))
    safe_write(om,15, 9,round(d['p101_hrs']+d['p102_hrs']+d['p103_hrs'],1))
    safe_write(om,15,10,d['pump_ute_combined'])
    ca = wb['Capacity Analysis']
    safe_write(ca,29,6,round(d['avg_trucks'],0)); safe_write(ca,29,7,round(d['avg_bbls'],0)); safe_write(ca,29,9,round(d['avg_bbls']*31,0))
    kt = wb['Key Takeaways']
    safe_write(kt,8,3, f"Feb 2026: 57.1 trucks/day, 11,200 bbls/day, 313,600 bbls for the month. "
        f"Mar 2026 (through {dt}): {d['avg_trucks']:.1f} trucks/day, {d['avg_bbls']:.1f} bbls/day run rate, "
        f"{d['proj_bbls']:,.0f} bbls projected. Total since April 2025: {yb:,.0f} bbls across {yt:,.0f} truck loads.")
    wb.save(output_path)
    print(f"  External report saved: {os.path.basename(output_path)}")

# ════════════════════════════════════════════════════════════════════════════
# BUILD EMAIL HTML — copied from local script (identical output)
# ════════════════════════════════════════════════════════════════════════════

def calc_switch_duration(start_str, end_str):
    try:
        fmt = '%I:%M%p'
        s = datetime.strptime(start_str.upper().replace(' ',''), fmt)
        e = datetime.strptime(end_str.upper().replace(' ',''), fmt)
        diff = (e - s).seconds // 60
        return (str(diff//60) + "hr " + str(diff%60) + "min") if diff >= 60 else (str(diff) + "min")
    except:
        return ""

def build_cadiz_section(switch_start, switch_end, loaded_out, empty_in, carrier_proj, maint_notes, carrier_actuals=None):
    """Build the Cadiz Ops Activity + Carrier Performance HTML section."""

    if carrier_actuals is None:
        carrier_actuals = {}

    has_content = any([switch_start, carrier_proj, carrier_actuals, maint_notes])
    if not has_content:
        return ""

    # Switch duration
    duration_str = ""
    if switch_start and switch_end:
        duration_str = calc_switch_duration(switch_start, switch_end)

    switch_html = ""
    if switch_start:
        cars_str = ""
        if loaded_out: cars_str += f" &nbsp;\u00b7&nbsp; {loaded_out} loaded out"
        if empty_in:   cars_str += f" / {empty_in} empty in"
        dur = f" &nbsp;\u00b7&nbsp; {duration_str}" if duration_str else ""
        switch_html = f'<div class="kv"><span class="lbl">Rail Switch</span><span class="val">{switch_start} \u2192 {switch_end}{dur}{cars_str}</span></div>'

    # Maintenance notes — only show if we have real maintenance items
    maint_html = ""
    if maint_notes:
        for note in maint_notes:
            maint_html += f'<div class="kv"><span class="lbl">Maintenance</span><span class="val" style="color:#90caf9;">{note[:150]}</span></div>'

    # Carrier Performance table — projected vs actual
    carrier_html = ""
    all_carriers = ['Badlands', 'KAG', 'Prop Logistics', 'BD Oil', '1st Choice Energy']
    total_proj_trucks = 0
    total_actual_trucks = 0
    total_actual_bbls = 0
    rows = ""

    for c in all_carriers:
        proj = carrier_proj.get(c, {})
        actual = carrier_actuals.get(c, {})
        proj_trucks = proj.get('trucks', 0)
        actual_trucks = actual.get('trucks', 0)
        actual_bbls = actual.get('bbls', 0)

        # Projection column
        if proj_trucks > 0:
            proj_str = str(proj_trucks)
            total_proj_trucks += proj_trucks
        elif proj.get('note') == 'No response':
            proj_str = '<span style="color:#666;font-style:italic;">\u2014</span>'
        else:
            proj_str = '0'

        # Actual column
        if actual_trucks > 0:
            actual_str = str(actual_trucks)
            bbls_str = f"{actual_bbls:,.0f}"
            total_actual_trucks += actual_trucks
            total_actual_bbls += actual_bbls
        else:
            actual_str = '<span style="color:#666;">0</span>'
            bbls_str = '<span style="color:#666;">\u2014</span>'

        # Variance column
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

        rows += f'<tr><td>{c}</td><td>{proj_str}</td><td>{actual_str}</td><td>{bbls_str}</td><td>{var_str}</td></tr>'

    # Total row
    proj_total_str = str(total_proj_trucks) if total_proj_trucks > 0 else '\u2014'
    total_var = total_actual_trucks - total_proj_trucks if total_proj_trucks > 0 else 0
    if total_proj_trucks > 0:
        if total_var > 0:
            var_total_str = f'<span style="color:#4caf50;">+{total_var}</span>'
        elif total_var < 0:
            var_total_str = f'<span style="color:#ef5350;">{total_var}</span>'
        else:
            var_total_str = '0'
    else:
        var_total_str = '\u2014'

    total_row = f'<tr class="tot"><td>Total</td><td>{proj_total_str}</td><td>{total_actual_trucks}</td><td>{total_actual_bbls:,.0f} BBLs</td><td>{var_total_str}</td></tr>'

    carrier_html = f"""
  <div class="sec-head" style="margin-top:10px;">\U0001f69b Carrier Performance</div>
  <table>
    <tr><th>Carrier</th><th>Projected</th><th>Actual</th><th>Actual BBLs</th><th>Variance</th></tr>
    {rows}
    {total_row}
  </table>"""

    return f"""
<div class="section">
  <div class="sec-head">\U0001f4e1 Cadiz Ops Activity</div>
  {switch_html}
  {maint_html}
  {carrier_html}
</div>"""

def build_email_html(d, dash_name, ext_name, cadiz_section=""):
    pu        = d['pump_ute']
    dt_str    = d['yesterday_date'].strftime('%B %#d, %Y')
    today_str = date.today().strftime('%A, %B %#d, %Y')

    yday_bbls   = sum(v['bbls']    for v in pu.values())
    yday_trucks = sum(v['loads']   for v in pu.values())
    yday_splits = sum(v['splits']  for v in pu.values())
    yday_rt     = sum(v['runtime'] for v in pu.values())
    combined_ute = yday_rt / (21*3) * 100 if yday_rt > 0 else 0
    bbl_hr_comb  = yday_bbls / yday_rt if yday_rt > 0 else 0
    bbl_per_truck = yday_bbls / yday_trucks if yday_trucks > 0 else 0

    vs_run  = (yday_bbls - d['avg_bbls']) / d['avg_bbls'] * 100 if d['avg_bbls'] > 0 else 0
    vs_feb  = d['proj_bbls'] - 313600
    mtd_vs  = (d['avg_bbls'] - 11200) / 11200 * 100

    def badge(val):
        c = '#4caf50' if val >= 0 else '#ef5350'
        s = '+' if val >= 0 else ''
        return f'<span style="color:{c}">{s}{val:.1f}%</span>'

    def badge_abs(val):
        c = '#4caf50' if val >= 0 else '#ef5350'
        s = '+' if val >= 0 else ''
        return f'<span style="color:{c}">{s}{val:,.0f} BBLs</span>'

    # ── Week 3 daily data ───────────────────────────────────────────────────
    week3_dates = [
        {'date': 'Mar 15 (Sat)', 'bbls': 8969.55,  'trucks': 45},
        {'date': 'Mar 16 (Sun)', 'bbls': 9567.05,  'trucks': 48},
        {'date': 'Mar 17 (Mon)', 'bbls': 9148.02,  'trucks': 45},
        {'date': 'Mar 18 (Tue)', 'bbls': 10449.12, 'trucks': 52},
        {'date': 'Mar 19 (Wed)', 'bbls': 8513.05,  'trucks': 43},
        {'date': 'Mar 20 (Thu)', 'bbls': 10734.38, 'trucks': 54},
        {'date': 'Mar 21 (Fri)', 'bbls': 11331.38, 'trucks': 56},
    ]
    week3_rows = ""
    for row in week3_dates:
        bpt  = row['bbls'] / row['trucks']
        vs   = (row['bbls'] - d['avg_bbls']) / d['avg_bbls'] * 100
        col  = '#4caf50' if vs >= 0 else '#ef5350'
        sign = '+' if vs >= 0 else ''
        week3_rows += f'<tr><td>{row["date"]}</td><td>{row["bbls"]:,.0f}</td><td>{row["trucks"]}</td><td>{bpt:.1f}</td><td style="color:{col}">{sign}{vs:.1f}%</td></tr>'

    week3_total_bbls   = sum(r['bbls']   for r in week3_dates)
    week3_total_trucks = sum(r['trucks'] for r in week3_dates)
    week3_total_days   = len(week3_dates)
    week3_bpt          = week3_total_bbls / week3_total_trucks
    week3_avg_vs       = (week3_total_bbls/week3_total_days - d['avg_bbls']) / d['avg_bbls'] * 100

    # ── Flags ────────────────────────────────────────────────────────────────
    flags = []
    # Soft streak check
    recent = week3_dates[-4:]
    soft   = all((r['bbls'] - d['avg_bbls']) / d['avg_bbls'] * 100 < 5 for r in recent)
    if soft:
        flags.append(('red', f"Mar 17\u201320 all running below run rate \u2014 4+ soft days in a row. Monitor Trafigura dispatch."))
    # Pump imbalance
    utes = {k: v['ute'] for k, v in pu.items()}
    max_p = max(utes, key=utes.get)
    min_p = min(utes, key=utes.get)
    if utes[max_p] - utes[min_p] > 10:
        flags.append(('yellow', f"{min_p} ute {utes[min_p]}% vs {max_p} {utes[max_p]}% \u2014 load imbalance across pumps. {max_p} carrying disproportionate share."))
    # BBLs/hr below avg
    low_hr = [f"{k} at {v['bbl_hr']:.0f}" for k, v in pu.items() if v['bbl_hr'] > 0 and v['bbl_hr'] < 430]
    if low_hr:
        flags.append(('yellow', f"BBLs/hr below Feb avg (438): {', '.join(low_hr)}. Consistent with gap time between trucks."))
    # Split count
    split_pct = (yday_splits / yday_trucks * 100) if yday_trucks > 0 else 0
    if split_pct <= 27:
        flags.append(('grn', f"Split count {yday_splits}/{yday_trucks} ({split_pct:.1f}%) \u2014 near historical avg (24%). No railcar changeover surge."))

    flags_html = ""
    for ftype, msg in flags:
        if ftype == 'red':
            flags_html += f'<div class="flag red">{msg}</div>'
        elif ftype == 'grn':
            flags_html += f'<div class="flag grn">{msg}</div>'
        else:
            flags_html += f'<div class="flag">{msg}</div>'

    return f"""<!DOCTYPE html>
<html><head><meta charset="UTF-8">
<style>
  body{{background:#1a1a1a;font-family:'Courier New',monospace;color:#e0e0e0;margin:0;padding:20px;}}
  .wrap{{max-width:640px;margin:0 auto;}}
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
  .foot{{color:#444;font-size:10px;text-align:center;margin-top:14px;border-top:1px solid #2a2a2a;padding-top:10px;}}
</style>
</head><body><div class="wrap">
<div class="title">\U0001f4ca Timiron Daily Briefing &nbsp;|&nbsp; Cadiz Terminal</div>
<div class="sub">{today_str} &nbsp;\u00b7&nbsp; Based on {dt_str} LOGS</div>

<div class="section">
  <div class="sec-head">\U0001f4c5 Yesterday \u2014 {dt_str}</div>
  <div class="kv"><span class="lbl">BBLs</span><span class="val">{yday_bbls:,.2f}</span></div>
  <div class="kv"><span class="lbl">Trucks</span><span class="val">{yday_trucks}</span></div>
  <div class="kv"><span class="lbl">Avg BBLs / Truck</span><span class="val">{bbl_per_truck:.1f}</span></div>
  <div class="kv"><span class="lbl">vs Run Rate Avg</span><span class="val">{badge(vs_run)} &nbsp;({yday_bbls:,.0f} vs {d['avg_bbls']:,.0f} avg)</span></div>
</div>

<div class="section">
  <div class="sec-head">\u2699\ufe0f Pump Ute \u2014 {dt_str} &nbsp;<span style="color:#555;font-size:10px;">(actual start/end times \u00b7 splits = first start \u2192 last end)</span></div>
  <table>
    <tr><th>Pump</th><th>Loads</th><th>Splits</th><th>Runtime</th><th>Ute %</th><th>BBLs</th><th>BBLs/Hr</th></tr>
    <tr><td>P-101</td><td>{pu['P-101']['loads']}</td><td>{pu['P-101']['splits']}</td><td>{pu['P-101']['runtime']} hrs</td><td>{pu['P-101']['ute']}%</td><td>{pu['P-101']['bbls']:,.0f}</td><td>{pu['P-101']['bbl_hr']:.0f}</td></tr>
    <tr><td>P-102</td><td>{pu['P-102']['loads']}</td><td>{pu['P-102']['splits']}</td><td>{pu['P-102']['runtime']} hrs</td><td>{pu['P-102']['ute']}%</td><td>{pu['P-102']['bbls']:,.0f}</td><td>{pu['P-102']['bbl_hr']:.0f}</td></tr>
    <tr><td>P-103</td><td>{pu['P-103']['loads']}</td><td>{pu['P-103']['splits']}</td><td>{pu['P-103']['runtime']} hrs</td><td>{pu['P-103']['ute']}%</td><td>{pu['P-103']['bbls']:,.0f}</td><td>{pu['P-103']['bbl_hr']:.0f}</td></tr>
    <tr class="tot"><td>Combined</td><td>{yday_trucks}</td><td>{yday_splits}</td><td>{yday_rt:.2f} hrs</td><td>{combined_ute:.1f}%</td><td>{yday_bbls:,.0f}</td><td>{bbl_hr_comb:.0f}</td></tr>
  </table>
  <div style="color:#555;font-size:10px;margin-top:6px;">Ute % = runtime \u00f7 21 true available hrs/pump (24hr \u2212 3hr rail switch) &nbsp;\u00b7&nbsp; Feb 2026 avg: 43.9%</div>
</div>

<div class="section">
  <div class="sec-head">\U0001f4c6 Month-to-Date (Mar 1\u2013{d['yesterday_date'].day})</div>
  <div class="kv"><span class="lbl">MTD Actuals</span><span class="val">{d['total_bbls']:,.0f} BBLs</span></div>
  <div class="kv"><span class="lbl">Daily Avg ({d['days_actual']} days)</span><span class="val">{d['avg_bbls']:,.1f} bbls/day</span></div>
  <div class="kv"><span class="lbl">vs Feb Avg (11,200/day)</span><span class="val">{badge(mtd_vs)}</span></div>
  <div class="kv"><span class="lbl">% of Rail Cap (15k/day)</span><span class="val" style="color:#90caf9">{d['rail_cap']*100:.1f}%</span></div>
  <div class="kv"><span class="lbl">Days Remaining</span><span class="val">{d['days_remain']}</span></div>
</div>

<div class="section">
  <div class="sec-head">\U0001f4c8 March Forecast ({d['days_actual']}-Day Run Rate)</div>
  <div class="kv"><span class="lbl">Run Rate Avg</span><span class="val">{d['avg_bbls']:,.1f} bbls/day</span></div>
  <div class="kv"><span class="lbl">Projected Total BBLs</span><span class="val">{d['proj_bbls']:,.0f}</span></div>
  <div class="kv"><span class="lbl">Projected Trucks</span><span class="val">~{d['proj_trucks']:,.0f}</span></div>
  <div class="kv"><span class="lbl">vs Feb Actual (313,600 BBLs)</span><span class="val">{badge_abs(vs_feb)}</span></div>
</div>

<div class="section">
  <div class="sec-head">\U0001f4ca Week 3 Status (Mar 15\u201321)</div>
  <table>
    <tr><th>Date</th><th>BBLs</th><th>Trucks</th><th>BBLs/Truck</th><th>vs Run Rate</th></tr>
    {week3_rows}
    <tr class="tot"><td>{week3_total_days}-Day Total</td><td>{week3_total_bbls:,.0f}</td><td>{week3_total_trucks}</td><td>{week3_bpt:.1f}</td><td style="color:#ef5350">{week3_avg_vs:.1f}% avg</td></tr>
  </table>
  <div style="color:#555;font-size:11px;margin-top:7px;">Week 1 (Mar 1\u20137): 74,752 BBLs &nbsp;\u00b7&nbsp; Week 2 (Mar 8\u201314): 78,513 BBLs</div>
</div>

{cadiz_section}
<div class="section">
  <div class="sec-head">\u26a0\ufe0f Flags</div>
  {flags_html}
  <div class="flag grn">\U0001f4ce Both Excel files attached \u2014 open directly in Excel.</div>
</div>

</div>

<div class="foot">
  Timiron Midstream Partners \u00b7 Cadiz Terminal, OH \u00b7 Auto-generated<br>
  Sent from tyk216@gmail.com \u00b7 {dash_name} \u00b7 {ext_name}
</div>
</div></body></html>"""

# ════════════════════════════════════════════════════════════════════════════
# SEND EMAIL VIA GMAIL SMTP — with both Excel files as real binary attachments
# ════════════════════════════════════════════════════════════════════════════

def send_via_gmail(subject, html_body, attachment_paths):
    msg = MIMEMultipart('mixed')
    msg['From']    = GMAIL_ADDRESS
    msg['To']      = ', '.join(RECIPIENTS)
    msg['Subject'] = subject

    msg.attach(MIMEText(html_body, 'html'))

    for path in attachment_paths:
        with open(path, 'rb') as f:
            part = MIMEBase('application', 'vnd.openxmlformats-officedocument.spreadsheetml.sheet')
            part.set_payload(f.read())
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', 'attachment',
                        filename=os.path.basename(path))
        msg.attach(part)

    with smtplib.SMTP_SSL('smtp.gmail.com', 465) as server:
        server.login(GMAIL_ADDRESS, GMAIL_APP_PASS)
        server.sendmail(GMAIL_ADDRESS, RECIPIENTS, msg.as_string())

    print(f"  Email sent with {len(attachment_paths)} Excel attachments.")

# ════════════════════════════════════════════════════════════════════════════
# MAIN
# ════════════════════════════════════════════════════════════════════════════

def main():
    today    = date.today()
    date_str = today.strftime('%#m-%#d-%y')

    print("=" * 62)
    print(f"  Timiron Cloud Briefing -- {today.strftime('%B %d, %Y')}")
    print("=" * 62)

    # Validate config
    if not MS_GRAPH_REFRESH_TOKEN:
        print("ERROR: MS_GRAPH_REFRESH_TOKEN not set"); sys.exit(1)
    if not MS_GRAPH_CLIENT_ID:
        print("ERROR: MS_GRAPH_CLIENT_ID not set"); sys.exit(1)
    if not GMAIL_APP_PASS:
        print("ERROR: GMAIL_APP_PASS not set"); sys.exit(1)

    # Use a temp directory for output files
    tmpdir = tempfile.mkdtemp(prefix="timiron_")

    # Step 0: Authenticate with Graph API
    print("\n[0] Authenticating with Microsoft Graph...")
    if not get_access_token():
        print("  ERROR: Could not authenticate with Microsoft Graph")
        sys.exit(1)

    # Step 1: Fetch Cadiz Ops data from Outlook
    print("\n[1] Fetching Cadiz Ops data from Outlook...")
    cadiz_data = fetch_cadiz_ops()
    if cadiz_data:
        print(f"  Switch: {cadiz_data.get('switch_start')} -> {cadiz_data.get('switch_end')}")
        print(f"  Carriers: {list(cadiz_data.get('carrier_projections',{}).keys())}")
    else:
        print("  Warning: No Cadiz ops data")

    # Step 2: Download Master Load Log from cadiz.ops LOGS email attachment
    print("\n[2] Downloading Master Load Log from cadiz.ops email...")
    excel_bytes, excel_filename = fetch_load_log_excel()
    if not excel_bytes:
        print("  ERROR: Could not download load log")
        sys.exit(1)

    # Step 3: Parse load log with pandas
    print("\n[3] Parsing load log...")
    d = parse_load_log(excel_bytes)

    # Step 4: Update Operations Dashboard Excel template
    print("\n[4] Updating Operations Dashboard...")
    dash_tpl = find_template("Operations_Dashboard_MASTER")
    dash_out = os.path.join(tmpdir, f"Timiron_Operations_Dashboard_MASTER_{date_str}.xlsx")
    update_dashboard(dash_tpl, d, dash_out)

    # Step 5: Update External Report Excel template
    print("\n[5] Updating External Report...")
    ext_tpl = find_template("External_Report")
    ext_out = os.path.join(tmpdir, f"Timiron_External_Report_{date_str}.xlsx")
    update_external_report(ext_tpl, d, ext_out)

    # Step 6: Build Cadiz ops section
    print("\n[6] Building Cadiz Ops section...")
    cadiz_section = ""
    if cadiz_data:
        switch_start = cadiz_data.get('switch_start')
        switch_end   = cadiz_data.get('switch_end')
        loaded_out   = cadiz_data.get('loaded_cars_out')
        empty_in     = cadiz_data.get('empty_cars_in')
        carrier_proj = cadiz_data.get('carrier_projections', {})
        maint_notes  = cadiz_data.get('maintenance_notes', [])
        carrier_actuals = d.get('carrier_actuals', {})
        cadiz_section = build_cadiz_section(switch_start, switch_end, loaded_out, empty_in, carrier_proj, maint_notes, carrier_actuals)
        print("  OK")
    else:
        print("  No Cadiz data available")

    # Step 7: Build HTML email and send
    print("\n[7] Building email and sending via Gmail...")
    dash_name = os.path.basename(dash_out)
    ext_name  = os.path.basename(ext_out)
    subject   = f"\U0001f4ca Timiron Daily Briefing | {today.strftime('%A, %B %d, %Y')}"
    html_body = build_email_html(d, dash_name, ext_name, cadiz_section)
    print(f"  HTML size: {len(html_body):,} bytes")

    send_via_gmail(subject, html_body, [dash_out, ext_out])

    print("\n" + "=" * 62)
    print("  Done.")
    print(f"  {dash_name}")
    print(f"  {ext_name}")
    print("=" * 62)

    # Clean up temp files
    try:
        shutil.rmtree(tmpdir)
    except:
        pass

if __name__ == "__main__":
    main()
