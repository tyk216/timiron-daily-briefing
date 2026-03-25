"""
timiron_cloud_briefing.py — Cloud version of Timiron Daily Briefing
Runs on GitHub Actions at 6 AM ET daily.
Uses Anthropic API + Zapier MCP for Outlook/OneDrive access.
Sends HTML briefing email via Gmail SMTP.
"""

import os, json, re, sys, smtplib, time
from datetime import date, timedelta, datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import requests

# ════════════════════════════════════════════════════════════════════════════
# CONFIG — from GitHub Secrets (environment variables)
# ════════════════════════════════════════════════════════════════════════════

ANTHROPIC_API_KEY = os.environ.get('ANTHROPIC_API_KEY', '')
ZAPIER_MCP_URL    = os.environ.get('ZAPIER_MCP_URL', '')
GMAIL_ADDRESS     = os.environ.get('GMAIL_ADDRESS', 'tyk216@gmail.com')
GMAIL_APP_PASS    = os.environ.get('GMAIL_APP_PASS', '')
RECIPIENTS        = os.environ.get('RECIPIENTS', 'tylerk@timironmp.com,robk@timirontrading.com').split(',')

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

def rev_per_day(bbls):
    return (min(bbls,5000)*1.30 + max(0,min(bbls-5000,5000))*0.95 + max(0,bbls-10000)*0.75)

# ════════════════════════════════════════════════════════════════════════════
# STEP 1: Fetch Cadiz Ops data via Claude + Zapier MCP (Outlook search)
# ════════════════════════════════════════════════════════════════════════════

def fetch_cadiz_ops():
    """Search Outlook for switch times and carrier projections."""
    today = date.today()
    yesterday = today - timedelta(days=1)
    today_str = today.strftime('%m.%d.%y')
    yesterday_str = yesterday.strftime('%m.%d.%y')
    today_long = today.strftime('%B %d, %Y')

    prompt = f"""Today is {today_long}. Yesterday was {yesterday_str}.

TASK: Search Outlook emails and extract Cadiz terminal operations data.

STEP 1: Use microsoft_outlook_find_emails with searchValue "{today_str} RAIL SWAP" to find the rail swap email.
Extract: START TIME, END TIME, loaded cars out, empty cars in.

STEP 2: Use microsoft_outlook_find_emails with searchValue "{today_str} UPDATE" to find the update email.
This tells us when normal operations resumed (switch_end time is in the subject, e.g. "4:48 AM UPDATE").

STEP 3: Use microsoft_outlook_find_emails with searchValue "Re: {today_str}" to find carrier replies.
Look for truck counts from: Badlands (ohiodispatch@badlands.com), KAG (bxi-bloomingdaledisp@thekag.com),
Prop Logistics, BD Oil, 1st Choice Energy.

Return ONLY valid JSON (no other text) with this structure:
{{
  "date": "{yesterday_str}",
  "switch_start": "3:05 AM",
  "switch_end": "4:48 AM",
  "loaded_cars_out": 17,
  "empty_cars_in": 17,
  "maintenance_notes": [],
  "carrier_projections": {{
    "Badlands": {{"trucks": 9, "proj_bbls": 2021, "note": ""}},
    "KAG": {{"trucks": 0, "proj_bbls": 0, "note": "No response"}}
  }}
}}

Use these avg BBLs/truck for projections: Badlands=224.6, KAG=188.9, Prop Logistics=181.5, BD Oil=188.2, 1st Choice Energy=183.3.
If a carrier didn't reply, set trucks=0 and note="No response".
"""

    try:
        r = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": ANTHROPIC_API_KEY,
                "anthropic-version": "2023-06-01",
                "anthropic-beta": "mcp-client-2025-04-04",
                "content-type": "application/json",
            },
            json={
                "model": "claude-sonnet-4-20250514",
                "max_tokens": 4000,
                "mcp_servers": [{"type": "url", "url": ZAPIER_MCP_URL, "name": "zapier"}],
                "messages": [{"role": "user", "content": prompt}]
            },
            timeout=120
        )
        if not r.ok:
            print(f"  Cadiz ops API error: {r.status_code} {r.text[:200]}")
            return None

        text = "".join(b.get("text", "") for b in r.json().get("content", []) if b.get("type") == "text")
        match = re.search(r'\{.*\}', text, re.DOTALL)
        if match:
            data = json.loads(match.group())
            data["date"] = yesterday_str
            print(f"  Switch: {data.get('switch_start')} -> {data.get('switch_end')}")
            carriers = data.get('carrier_projections', {})
            active = [k for k, v in carriers.items() if v.get('trucks', 0) > 0]
            print(f"  Carriers responding: {active}")
            return data
        print(f"  Warning: Could not parse JSON from response")
        print(f"  Response text: {text[:300]}")
        return None
    except Exception as e:
        print(f"  Cadiz ops fetch error: {e}")
        return None

# ════════════════════════════════════════════════════════════════════════════
# STEP 2: Fetch Master Load Log data via Claude + Zapier MCP (OneDrive/Excel)
# ════════════════════════════════════════════════════════════════════════════

def fetch_load_log_data():
    """Read Master Load Log data from OneDrive Excel via Zapier MCP."""
    yesterday = date.today() - timedelta(days=1)
    yesterday_str = yesterday.strftime('%m.%d.%y')

    prompt = f"""TASK: Find and read the latest Master Load Log from OneDrive.

STEP 1: Use onedrive_find_file with query "MASTER COPY MASTER LOAD LOG" to find the latest load log file.
The file is in the Timiron/Claude/Daily Brief folder. Look for one with "{yesterday_str}" in the name.

STEP 2: Once you find the file, use microsoft_excel_get_cells_in_range to read data.
The workbook has a sheet called "Master_Load_Log".
Read range A1:J500 to get all March 2026 data.
The columns are: Date, Timiron BOL#, Truck #, Carrier, Timiron Metered bbls., Pump Time, Split Load, and more.

STEP 3: Process the data and return ONLY valid JSON (no other text):
{{
  "yesterday_date": "{yesterday_str}",
  "yesterday_bbls": 11474.24,
  "yesterday_trucks": 59,
  "mtd_days": 24,
  "mtd_total_bbls": 253109.38,
  "mtd_total_trucks": 1234,
  "avg_bbls_per_day": 10546.2,
  "pump_utilization": {{
    "P-101": {{"loads": 25, "splits": 6, "runtime_hrs": 10.03, "ute_pct": 47.8, "bbls": 4744, "bbls_hr": 473}},
    "P-102": {{"loads": 22, "splits": 5, "runtime_hrs": 10.05, "ute_pct": 47.9, "bbls": 4495, "bbls_hr": 447}},
    "P-103": {{"loads": 12, "splits": 2, "runtime_hrs": 5.4, "ute_pct": 25.7, "bbls": 2236, "bbls_hr": 414}}
  }},
  "daily_data": [
    {{"date": "2026-03-01", "bbls": 10500, "trucks": 52}},
    ...
  ]
}}

IMPORTANT:
- BOL# starting with 111 = P-101, 222 = P-102, 333 = P-103
- Pump utilization % = runtime hours / 21 available hours * 100
- Split loads have "Split #2" in the Split Load column - count them but don't double-count trucks
- Pump Time format is H:MM - convert to hours
- Only include March 2026 data
- "trucks" = unique loads excluding Split #2 rows
"""

    try:
        r = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": ANTHROPIC_API_KEY,
                "anthropic-version": "2023-06-01",
                "anthropic-beta": "mcp-client-2025-04-04",
                "content-type": "application/json",
            },
            json={
                "model": "claude-sonnet-4-20250514",
                "max_tokens": 8000,
                "mcp_servers": [{"type": "url", "url": ZAPIER_MCP_URL, "name": "zapier"}],
                "messages": [{"role": "user", "content": prompt}]
            },
            timeout=180
        )
        if not r.ok:
            print(f"  Load log API error: {r.status_code} {r.text[:200]}")
            return None

        text = "".join(b.get("text", "") for b in r.json().get("content", []) if b.get("type") == "text")
        match = re.search(r'\{.*\}', text, re.DOTALL)
        if match:
            data = json.loads(match.group())
            print(f"  Yesterday: {data.get('yesterday_bbls')} BBLs, {data.get('yesterday_trucks')} trucks")
            print(f"  MTD: {data.get('mtd_days')} days, {data.get('mtd_total_bbls')} BBLs")
            return data
        print(f"  Warning: Could not parse load log JSON")
        return None
    except Exception as e:
        print(f"  Load log fetch error: {e}")
        return None

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
    if not ANTHROPIC_API_KEY:
        print("ERROR: ANTHROPIC_API_KEY not set"); sys.exit(1)
    if not ZAPIER_MCP_URL:
        print("ERROR: ZAPIER_MCP_URL not set"); sys.exit(1)
    if not GMAIL_APP_PASS:
        print("ERROR: GMAIL_APP_PASS not set"); sys.exit(1)

    # Step 1: Fetch Cadiz Ops data from Outlook
    print("\n[1] Fetching Cadiz Ops data from Outlook...")
    cadiz_data = fetch_cadiz_ops()
    if cadiz_data:
        print("  OK")
    else:
        print("  Warning: No Cadiz ops data - will show N/A in briefing")

    # Step 2: Fetch Master Load Log data from OneDrive
    print("\n[2] Fetching Master Load Log from OneDrive...")
    load_data = fetch_load_log_data()
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
