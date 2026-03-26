"""
timiron_cloud_briefing.py — Timiron Daily Briefing (v2)

Pulls load log directly from Cadiz Ops OneDrive via Graph API.
Scans Outlook emails for rail swap, maintenance, and carrier data.
Builds dark-themed HTML briefing + Excel attachments, sends via Gmail.

All month-specific constants live in config.yaml — no code changes needed month to month.
Parse logic shared with PWA dashboard via parse_loadlog.py.
"""

import os, re, sys, shutil, smtplib, tempfile, calendar
from datetime import date, timedelta, datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from pathlib import Path

import openpyxl

# Shared module — auth, fetch, parse
from parse_loadlog import (
    CFG, current_month_info, rev_per_day,
    get_access_token, graph_headers,
    search_emails, get_attachments, get_body_text,
    fetch_load_log_from_onedrive, parse_load_log,
)

# ════════════════════════════════════════════════════════════════════════════
# ENV VARS — from GitHub Secrets
# ════════════════════════════════════════════════════════════════════════════

GMAIL_ADDRESS  = os.environ.get('GMAIL_ADDRESS', 'tyk216@gmail.com')
GMAIL_APP_PASS = os.environ.get('GMAIL_APP_PASS', '')
RECIPIENTS     = os.environ.get('RECIPIENTS', 'tylerk@timironmp.com,robk@timirontrading.com').split(',')

SCRIPT_DIR   = Path(__file__).parent
TEMPLATE_DIR = SCRIPT_DIR / "templates"

# ════════════════════════════════════════════════════════════════════════════
# FETCH OPS DATA FROM EMAIL — rail swaps, updates, carrier projections
# ════════════════════════════════════════════════════════════════════════════

def fetch_email_ops_data():
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
        "switch_start": None, "switch_end": None,
        "loaded_cars_out": 0, "empty_cars_in": 0,
        "maintenance_notes": [], "carrier_projections": {}, "email_errors": [],
    }

    # RAIL SWAP
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

    # UPDATE
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

    # Carrier projections
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
                "trucks": trucks, "proj_bbls": 0, "note": "" if responded else "No response today", "responded": responded,
            }
        except Exception as e:
            print(f"  Warning: {cname} carrier search failed: {e}")
            result["email_errors"].append(f"{cname} search failed: {e}")
            result["carrier_projections"][cname] = {"trucks": 0, "proj_bbls": 0, "note": "Search error", "responded": False}

    for carrier in carriers_cfg:
        cname = carrier['name']
        if cname not in result["carrier_projections"]:
            result["carrier_projections"][cname] = {"trucks": 0, "proj_bbls": 0, "note": "No response", "responded": False}

    return result

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
# BUILD EMAIL HTML
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

    maint_html = ""
    for note in maint_notes:
        maint_html += f'<div class="kv"><span class="lbl">Maintenance</span><span class="val" style="color:#90caf9;">{note[:150]}</span></div>'

    error_html = ""
    for err in email_errors:
        error_html += f'<div class="kv"><span class="lbl" style="color:#ef5350;">Email Error</span><span class="val" style="color:#ef5350;font-size:11px;">{err}</span></div>'

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

        if proj_trucks > 0:
            proj_str = str(proj_trucks)
            total_proj_trucks += proj_trucks
        elif not responded and proj.get('note'):
            proj_str = '<span style="color:#ef5350;font-size:10px;">No reply</span>'
        else:
            proj_str = '<span style="color:#666;">\u2014</span>'

        if actual_trucks > 0:
            actual_str = str(actual_trucks)
            bbls_str = f"{actual_bbls:,.0f}"
            total_actual_trucks += actual_trucks
            total_actual_bbls += actual_bbls
        else:
            actual_str = '<span style="color:#666;">0</span>'
            bbls_str = '<span style="color:#666;">\u2014</span>'

        mtd_avg_str = f"{rolling['avg_trucks_per_day']:.1f} / {rolling['avg_bbls_per_truck']:.0f}" if rolling else '<span style="color:#666;">\u2014</span>'

        if proj_trucks > 0 and actual_trucks > 0:
            var = actual_trucks - proj_trucks
            if var > 0: var_str = f'<span style="color:#4caf50;">+{var}</span>'
            elif var < 0: var_str = f'<span style="color:#ef5350;">{var}</span>'
            else: var_str = '<span style="color:#888;">0</span>'
        elif proj_trucks == 0 and actual_trucks > 0: var_str = '<span style="color:#888;">\u2014</span>'
        elif proj_trucks > 0 and actual_trucks == 0: var_str = f'<span style="color:#ef5350;">-{proj_trucks}</span>'
        else: var_str = '<span style="color:#666;">\u2014</span>'

        rows += f'<tr><td>{c}</td><td>{proj_str}</td><td>{actual_str}</td><td>{bbls_str}</td><td style="font-size:10px;">{mtd_avg_str}</td><td>{var_str}</td></tr>'

    proj_total_str = str(total_proj_trucks) if total_proj_trucks > 0 else '\u2014'
    total_var = total_actual_trucks - total_proj_trucks if total_proj_trucks > 0 else 0
    if total_proj_trucks > 0:
        if total_var > 0: var_total = f'<span style="color:#4caf50;">+{total_var}</span>'
        elif total_var < 0: var_total = f'<span style="color:#ef5350;">{total_var}</span>'
        else: var_total = '0'
    else: var_total = '\u2014'

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

    # 5-day trend
    trend_rows = ""
    trend = d.get('day_trend', [])
    for i, day in enumerate(trend):
        dd = day['date']
        day_label = f"{dd.strftime('%b %#d')} ({dd.strftime('%a')})"
        bbls = day['bbls']
        trucks = day['trucks']
        bpt = bbls / trucks if trucks > 0 else 0
        vs = (bbls - d['avg_bbls']) / d['avg_bbls'] * 100 if d['avg_bbls'] > 0 else 0
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

    # Weekly breakdown
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

    # Weekend/weekday
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

    # Pump rows
    pump_rows = ""
    for pump in CFG.get('operations', {}).get('pumps', []):
        pname = pump['name']
        p = pu.get(pname, {})
        pump_rows += (
            f'<tr><td>{pname}</td><td>{p.get("loads", 0)}</td><td>{p.get("splits", 0)}</td>'
            f'<td>{p.get("runtime", 0)} hrs</td><td>{p.get("ute", 0)}%</td>'
            f'<td>{p.get("bbls", 0):,.0f}</td><td>{p.get("bbl_hr", 0):.0f}</td></tr>'
        )

    # Flags
    flag_cfg = CFG.get('flags', {})
    flags = []
    soft_threshold = flag_cfg.get('soft_day_threshold_pct', 5)
    streak_len = flag_cfg.get('soft_streak_days', 4)
    if len(trend) >= streak_len:
        recent = trend[-streak_len:]
        if all((r['bbls'] - d['avg_bbls']) / d['avg_bbls'] * 100 < -soft_threshold for r in recent if d['avg_bbls'] > 0):
            date_range = f"{recent[0]['date'].strftime('%b %#d')}\u2013{recent[-1]['date'].strftime('%#d')}"
            flags.append(('red', f"{date_range} all running below run rate \u2014 {streak_len}+ soft days in a row."))

    utes = {k: v['ute'] for k, v in pu.items() if v['runtime'] > 0}
    if len(utes) >= 2:
        max_p = max(utes, key=utes.get)
        min_p = min(utes, key=utes.get)
        if utes[max_p] - utes[min_p] > flag_cfg.get('pump_imbalance_threshold_pct', 10):
            flags.append(('yellow', f"{min_p} ute {utes[min_p]}% vs {max_p} {utes[max_p]}% \u2014 load imbalance."))

    low_hr_thresh = flag_cfg.get('low_bbls_per_hr', 430)
    low_hr = [f"{k} at {v['bbl_hr']:.0f}" for k, v in pu.items() if v['bbl_hr'] > 0 and v['bbl_hr'] < low_hr_thresh]
    if low_hr:
        flags.append(('yellow', f"BBLs/hr below avg ({low_hr_thresh}): {', '.join(low_hr)}."))

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

    source_html = f'<div style="color:#555;font-size:10px;margin-top:4px;">{data_source_info}</div>' if data_source_info else ""
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
# SEND EMAIL
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

    if not os.environ.get('MS_GRAPH_REFRESH_TOKEN'):
        print("ERROR: MS_GRAPH_REFRESH_TOKEN not set"); sys.exit(1)
    if not os.environ.get('MS_GRAPH_CLIENT_ID'):
        print("ERROR: MS_GRAPH_CLIENT_ID not set"); sys.exit(1)
    if not GMAIL_APP_PASS:
        print("ERROR: GMAIL_APP_PASS not set"); sys.exit(1)

    mi = current_month_info()
    print(f"\n  Month: {mi['month_name']} {mi['year']} ({mi['days_in_month']} days)")
    print(f"  Fixed cost: ${mi['fixed_cost']:,.2f}")

    tmpdir = tempfile.mkdtemp(prefix="timiron_")

    print("\n[0] Authenticating with Microsoft Graph...")
    if not get_access_token():
        print("  FATAL: Could not authenticate"); sys.exit(1)

    print("\n[1] Fetching load log from Cadiz Ops OneDrive...")
    excel_bytes, excel_filename, last_modified = fetch_load_log_from_onedrive()
    if not excel_bytes:
        print("  FATAL: Could not obtain load log"); sys.exit(1)

    data_source_info = f"Source: {excel_filename}"
    if last_modified:
        stale_hrs = CFG.get('flags', {}).get('stale_data_hours', 18)
        age_hrs = (datetime.now(last_modified.tzinfo) - last_modified).total_seconds() / 3600
        mod_str = last_modified.strftime('%b %#d %I:%M %p')
        data_source_info += f" &nbsp;\u00b7&nbsp; Last saved: {mod_str}"
        if age_hrs > stale_hrs:
            data_source_info += f' &nbsp;\u00b7&nbsp; <span style="color:#ef5350;">STALE ({age_hrs:.0f}hrs old)</span>'
            print(f"  WARNING: Load log is {age_hrs:.0f} hours old")
        else:
            print(f"  Data age: {age_hrs:.1f} hours (OK)")

    print("\n[2] Scanning Outlook for operational updates...")
    ops_data = fetch_email_ops_data()
    if ops_data.get('email_errors'):
        print(f"  Partial email data ({len(ops_data['email_errors'])} errors)")
    else:
        print("  Email scan complete")

    print("\n[3] Parsing load log...")
    d = parse_load_log(excel_bytes, mi)

    for cname, proj in ops_data.get('carrier_projections', {}).items():
        rolling = d['carrier_rolling_avgs'].get(cname, {})
        avg_bpt = rolling.get('avg_bbls_per_truck', 190)
        proj['proj_bbls'] = round(proj['trucks'] * avg_bpt)

    print("\n[4] Updating Operations Dashboard...")
    dash_tpl = find_template("Operations_Dashboard_MASTER")
    dash_out = os.path.join(tmpdir, f"Timiron_Operations_Dashboard_MASTER_{date_str}.xlsx")
    update_dashboard(dash_tpl, d, mi, dash_out)

    print("\n[5] Updating External Report...")
    ext_tpl = find_template("External_Report")
    ext_out = os.path.join(tmpdir, f"Timiron_External_Report_{date_str}.xlsx")
    update_external_report(ext_tpl, d, mi, ext_out)

    print("\n[6] Building email...")
    cadiz_section = build_cadiz_section(ops_data, d.get('carrier_actuals', {}), d.get('carrier_rolling_avgs', {}))
    dash_name = os.path.basename(dash_out)
    ext_name = os.path.basename(ext_out)
    html_body = build_email_html(d, mi, dash_name, ext_name, cadiz_section, data_source_info)
    print(f"  HTML: {len(html_body):,} bytes")

    print("\n[7] Sending via Gmail...")
    subject = f"\U0001f4ca Timiron Daily Briefing | {today.strftime('%A, %B %d, %Y')}"
    send_via_gmail(subject, html_body, [dash_out, ext_out])

    print("\n" + "=" * 62)
    print("  DONE")
    print("=" * 62)


if __name__ == "__main__":
    main()
