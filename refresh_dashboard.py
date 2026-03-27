"""Auto-refresh Cadiz Ops dashboard from OneDrive API."""
import requests, json, os, subprocess
from collections import defaultdict
from datetime import datetime, timedelta

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CLIENT_ID = '14d82eec-204b-4c2f-b7e8-296a70dab67e'
TOKEN_FILE = os.path.join(SCRIPT_DIR, '_cadiz_ops_refresh_token.txt')
JSON_FILE = os.path.join(SCRIPT_DIR, 'pwa', 'data', 'dashboard.json')
ITEM_ID = None  # Auto-detected from most recent MASTER COPY LOAD LOG file
PWA_DIR = os.path.join(SCRIPT_DIR, 'pwa')

# Excel serial date for Jan 1 2026 = 46023
# March 1 2026 = 46082
EXCEL_EPOCH = datetime(1899, 12, 30)

def find_load_log(token):
    """Find the most recently modified MASTER COPY LOAD LOG file across entire drive."""
    headers = {'Authorization': f'Bearer {token}'}
    # Search entire drive for load log files
    r = requests.get(
        "https://graph.microsoft.com/v1.0/me/drive/root/search(q='MASTER COPY LOAD LOG')",
        headers=headers, timeout=15
    )
    if not r.ok:
        raise Exception(f"Could not search drive: {r.status_code} {r.text[:200]}")

    # Find the most recently modified .xlsx file matching the pattern
    best = None
    for item in r.json().get('value', []):
        name = item['name'].upper()
        if 'MASTER COPY' in name and 'LOAD LOG' in name and name.endswith('.XLSX'):
            if best is None or item['lastModifiedDateTime'] > best['lastModifiedDateTime']:
                best = item

    if best:
        print(f'  Found load log: {best["name"]} (modified {best["lastModifiedDateTime"]})')
        return best['id']
    raise Exception("No MASTER COPY LOAD LOG file found on drive")

def serial_to_date(serial):
    return EXCEL_EPOCH + timedelta(days=int(serial))

def date_to_serial(dt):
    return (dt - EXCEL_EPOCH).days

def get_token():
    rt = open(TOKEN_FILE).read().strip()
    r = requests.post('https://login.microsoftonline.com/common/oauth2/v2.0/token', data={
        'client_id': CLIENT_ID, 'grant_type': 'refresh_token',
        'refresh_token': rt, 'scope': 'offline_access Files.Read.All Sites.Read.All',
    })
    td = r.json()
    if 'access_token' not in td:
        raise Exception(f"Token refresh failed: {td.get('error_description', td)}")
    with open(TOKEN_FILE, 'w') as f:
        f.write(td['refresh_token'])
    return td['access_token']

def read_march_data(token, item_id=None):
    headers = {'Authorization': f'Bearer {token}'}
    now = datetime.utcnow() - timedelta(hours=6)  # CST
    month_start = now.replace(day=1, hour=0, minute=0, second=0)
    start_serial = date_to_serial(month_start)
    today_serial = date_to_serial(now)

    file_id = item_id or find_load_log(token)

    all_rows = []
    for cs in range(2, 6000, 500):
        ce = cs + 499
        url = f'https://graph.microsoft.com/v1.0/me/drive/items/{file_id}/workbook/worksheets(%27Master_Load_Log%27)/range(address=%27A{cs}:X{ce}%27)'
        r = requests.get(url, headers=headers, timeout=30)
        if not r.ok:
            continue
        for row in r.json().get('values', []):
            if row[1] and isinstance(row[1], (int, float)):
                if start_serial <= row[1] <= today_serial + 1:
                    all_rows.append(row)
        # If we've passed our date range, stop
        last_dates = [row[1] for row in r.json().get('values', []) if row[1] and isinstance(row[1], (int, float))]
        if last_dates and max(last_dates) > today_serial + 5:
            break
        if not last_dates and cs > 4000:
            break

    return all_rows, start_serial, today_serial, now

def calculate_kpis(rows, start_serial, today_serial, now):
    daily = defaultdict(lambda: {
        'bbls': 0, 'trucks': 0, 'api_sum': 0, 'bsw_sum': 0, 'n': 0,
        'pump_sum': 0, 'pump_n': 0, 'splits': 0,
        'carriers': defaultdict(lambda: {'trucks': 0, 'bbls': 0}),
        'pumps': defaultdict(lambda: {'loads': 0, 'splits': 0, 'runtime': 0, 'bbls': 0})
    })

    for row in rows:
        try:
            day = int(row[1])
            carrier = str(row[2]).strip() if row[2] else 'Unknown'
            bbls = float(row[17]) if row[17] else 0
            api = float(row[13]) if row[13] else 0
            bsw = float(row[14]) if row[14] else 0
            split = str(row[16]).strip().lower() if row[16] else ''
            pump_time = float(row[22]) if row[22] else 0
            # Col X (index 23) = Timiron BOL# → pump ID
            bol = str(row[23]).strip() if row[23] else ''
            pump_id = None
            if bol.startswith('111'):
                pump_id = 'P-101'
            elif bol.startswith('222'):
                pump_id = 'P-102'
            elif bol.startswith('333'):
                pump_id = 'P-103'

            d = daily[day]
            d['bbls'] += bbls

            is_split2 = split == 'split #2'
            if not is_split2:
                d['trucks'] += 1
                d['carriers'][carrier]['trucks'] += 1
            else:
                d['splits'] += 1
            d['carriers'][carrier]['bbls'] += bbls

            # Pump utilization
            if pump_id:
                p = d['pumps'][pump_id]
                p['loads'] += 1
                if is_split2:
                    p['splits'] += 1
                p['bbls'] += bbls
                if pump_time > 0:
                    p['runtime'] += pump_time * 24  # Convert from fraction of day to hours

            if api > 0:
                d['api_sum'] += api
                d['bsw_sum'] += bsw
                d['n'] += 1
            if pump_time > 0:
                d['pump_sum'] += pump_time
                d['pump_n'] += 1
        except (ValueError, TypeError):
            continue

    # Find latest day and previous day
    sorted_days = sorted(daily.keys())
    latest = sorted_days[-1] if sorted_days else today_serial
    t = daily[latest]
    latest_date = serial_to_date(latest)

    # Yesterday (second most recent day)
    prev_day = sorted_days[-2] if len(sorted_days) >= 2 else None
    yesterday_data = None
    if prev_day:
        pd = daily[prev_day]
        prev_date = serial_to_date(prev_day)
        yesterday_data = {
            'date': prev_date.strftime('%Y-%m-%d'),
            'bbls': round(pd['bbls'], 2),
            'trucks': pd['trucks'],
            'splits': pd['splits'],
            'avg_api': round(pd['api_sum'] / pd['n'], 2) if pd['n'] > 0 else 0,
            'avg_bsw': round(pd['bsw_sum'] / pd['n'] * 100, 2) if pd['n'] > 0 else 0,
        }

    # Today KPIs
    today_data = {
        'date': latest_date.strftime('%Y-%m-%d'),
        'bbls': round(t['bbls'], 2),
        'trucks': t['trucks'],
        'splits': t['splits'],
        'live': True
    }

    # MTD
    mtd_bbls = sum(d['bbls'] for d in daily.values())
    mtd_trucks = sum(d['trucks'] for d in daily.values())
    days_actual = len(daily)
    days_in_month = 31 if now.month == 3 else 30
    days_remain = days_in_month - days_actual
    avg_bbls = mtd_bbls / days_actual if days_actual > 0 else 0

    # Projection
    proj_bbls = avg_bbls * days_in_month
    rev_per_bbl = 1.1032
    proj_rev = proj_bbls * rev_per_bbl
    fixed_cost = 244583.5 / 12 * (days_in_month / 30)

    # Weekly breakdown
    weeks = []
    if sorted_days:
        first_date = serial_to_date(sorted_days[0])
        # Week 1 starts on the 1st
        week_start = first_date
        week_num = 1
        week_data = {'bbls': 0, 'trucks': 0, 'days': 0, 'start': None, 'end': None}
        for dk in sorted_days:
            dt = serial_to_date(dk)
            # New week on Sunday (weekday 6) after week 1, or every 7 days
            if dt.weekday() == 6 and week_data['days'] > 0 and week_num > 0:
                # Save current week
                avg = week_data['bbls'] / week_data['days'] if week_data['days'] > 0 else 0
                bpt = week_data['bbls'] / week_data['trucks'] if week_data['trucks'] > 0 else 0
                weeks.append({
                    'week_num': week_num,
                    'start': week_data['start'],
                    'end': week_data['end'],
                    'total_bbls': round(week_data['bbls'], 0),
                    'total_trucks': week_data['trucks'],
                    'days': week_data['days'],
                    'avg_bbls': round(avg, 1),
                    'bpt': round(bpt, 1)
                })
                week_num += 1
                week_data = {'bbls': 0, 'trucks': 0, 'days': 0, 'start': None, 'end': None}
            dd = daily[dk]
            week_data['bbls'] += dd['bbls']
            week_data['trucks'] += dd['trucks']
            week_data['days'] += 1
            if not week_data['start']:
                week_data['start'] = dt.strftime('%Y-%m-%d')
            week_data['end'] = dt.strftime('%Y-%m-%d')
        # Save final week
        if week_data['days'] > 0:
            avg = week_data['bbls'] / week_data['days'] if week_data['days'] > 0 else 0
            bpt = week_data['bbls'] / week_data['trucks'] if week_data['trucks'] > 0 else 0
            weeks.append({
                'week_num': week_num,
                'start': week_data['start'],
                'end': week_data['end'],
                'total_bbls': round(week_data['bbls'], 0),
                'total_trucks': week_data['trucks'],
                'days': week_data['days'],
                'avg_bbls': round(avg, 1),
                'bpt': round(bpt, 1)
            })

    # 5-day trend
    last5 = sorted_days[-5:]
    trend = []
    for dk in last5:
        dd = daily[dk]
        trend.append({
            'date': serial_to_date(dk).strftime('%Y-%m-%d'),
            'bbls': round(dd['bbls'], 2),
            'trucks': dd['trucks']
        })

    # Carrier rolling averages
    carrier_rolling = defaultdict(lambda: {'total_trucks': 0, 'total_bbls': 0})
    carrier_today = {}
    for dk, dd in daily.items():
        for c, cv in dd['carriers'].items():
            carrier_rolling[c]['total_trucks'] += cv['trucks']
            carrier_rolling[c]['total_bbls'] += cv['bbls']
        if dk == latest:
            for c, cv in dd['carriers'].items():
                carrier_today[c] = {'trucks': cv['trucks'], 'bbls': round(cv['bbls'], 1)}

    carrier_avgs = {}
    for c, cv in carrier_rolling.items():
        carrier_avgs[c] = {
            'avg_bbls_per_truck': round(cv['total_bbls'] / cv['total_trucks'], 1) if cv['total_trucks'] > 0 else 0,
            'avg_trucks_per_day': round(cv['total_trucks'] / days_actual, 1),
            'total_trucks': cv['total_trucks'],
            'total_bbls': round(cv['total_bbls'], 1)
        }

    # Weekday vs weekend
    wday_bbls = wday_days = wkend_bbls = wkend_days = 0
    wday_trucks = wkend_trucks = 0
    for dk, dd in daily.items():
        dt = serial_to_date(dk)
        if dt.weekday() < 5:
            wday_bbls += dd['bbls']
            wday_trucks += dd['trucks']
            wday_days += 1
        else:
            wkend_bbls += dd['bbls']
            wkend_trucks += dd['trucks']
            wkend_days += 1

    avg_api = t['api_sum'] / t['n'] if t['n'] > 0 else 0
    avg_bsw = t['bsw_sum'] / t['n'] if t['n'] > 0 else 0
    avg_pump_min = (t['pump_sum'] / t['pump_n'] * 24 * 60) if t['pump_n'] > 0 else 0

    dashboard = {
        'generated_at': datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'),
        'source_file': 'MASTER COPY - FEB MASTER LOAD LOG (API)',
        'source_last_modified': datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S+00:00'),
        'terminal': 'Cadiz Terminal',
        'company': 'Timiron Midstream Partners',
        'month': now.strftime('%B'),
        'month_abbr': now.strftime('%b'),
        'year': now.year,
        'days_in_month': days_in_month,
        'yesterday': today_data,
        'yesterday_actual': yesterday_data,
        'pump_utilization': {
            p: {
                'loads': pv['loads'],
                'splits': pv['splits'],
                'runtime': round(pv['runtime'], 2),
                'ute': round(pv['runtime'] / 21 * 100, 1) if pv['runtime'] > 0 else 0,
                'bbls': round(pv['bbls'], 0),
                'bbl_hr': round(pv['bbls'] / pv['runtime'], 0) if pv['runtime'] > 0 else 0
            }
            for p, pv in t['pumps'].items()
        },
        'pump_available_hrs': 21,
        'mtd': {
            'total_bbls': round(mtd_bbls, 2),
            'total_trucks': mtd_trucks,
            'days_actual': days_actual,
            'days_remain': days_remain,
            'avg_bbls': round(avg_bbls, 1),
            'avg_trucks': round(mtd_trucks / days_actual, 1) if days_actual > 0 else 0,
            'rail_cap_pct': round(avg_bbls / 15000 * 100, 1)
        },
        'projection': {
            'proj_bbls': round(proj_bbls),
            'proj_trucks': round(mtd_trucks / days_actual * days_in_month) if days_actual > 0 else 0,
            'proj_rev': round(proj_rev),
            'ebitda': round(proj_rev - fixed_cost)
        },
        'prior_month': {
            'name': 'Feb',
            'total_bbls': 313600,
            'avg_bbls_per_day': 11200
        },
        'day_trend': trend,
        'weeks': weeks,
        'carrier_actuals': carrier_today,
        'carrier_rolling_avgs': carrier_avgs,
        'wday_wkend': {
            'weekday': {
                'days': wday_days,
                'total_bbls': round(wday_bbls, 1),
                'avg_bbls': round(wday_bbls / wday_days, 1) if wday_days > 0 else 0,
                'total_trucks': wday_trucks,
                'avg_trucks': round(wday_trucks / wday_days, 1) if wday_days > 0 else 0
            },
            'weekend': {
                'days': wkend_days,
                'total_bbls': round(wkend_bbls, 1),
                'avg_bbls': round(wkend_bbls / wkend_days, 1) if wkend_days > 0 else 0,
                'total_trucks': wkend_trucks,
                'avg_trucks': round(wkend_trucks / wkend_days, 1) if wkend_days > 0 else 0
            }
        },
        'config': {
            'pump_available_hrs': 21,
            'rail_cap_daily_bbls': 15000,
            'pumps': ['P-101', 'P-102', 'P-103'],
            'carriers': list(carrier_avgs.keys())
        }
    }

    return dashboard

def deploy():
    result = subprocess.run(
        'npx wrangler pages deploy . --project-name=cadiz-ops --branch=main --commit-dirty=true',
        cwd=PWA_DIR, capture_output=True, timeout=60, shell=True
    )
    out = (result.stdout or b'').decode('utf-8', errors='replace') + (result.stderr or b'').decode('utf-8', errors='replace')
    return result.returncode == 0, out

def main():
    print(f'[{datetime.utcnow().isoformat()}] Refreshing dashboard...')

    token = get_token()
    print('  Token obtained')

    rows, start_s, today_s, now = read_march_data(token)
    print(f'  Read {len(rows)} rows')

    dashboard = calculate_kpis(rows, start_s, today_s, now)
    print(f'  Today: {dashboard["yesterday"]["bbls"]} bbls / {dashboard["yesterday"]["trucks"]} trucks')
    print(f'  MTD: {dashboard["mtd"]["total_bbls"]} bbls / {dashboard["mtd"]["total_trucks"]} trucks')

    with open(JSON_FILE, 'w') as f:
        json.dump(dashboard, f, indent=2)
    print('  JSON updated')

    ok, output = deploy()
    if ok:
        print('  Deployed to cadiz-ops.pages.dev')
    else:
        print(f'  Deploy failed: {output[-200:]}')

    return dashboard

if __name__ == '__main__':
    main()
