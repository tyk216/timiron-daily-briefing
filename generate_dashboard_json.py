"""
generate_dashboard_json.py — Generate dashboard.json for the Cadiz Terminal PWA.

Shares parse logic with timiron_cloud_briefing.py via parse_loadlog.py.
Outputs JSON to stdout (piped to file by GitHub Action).
"""

import os, sys, json, calendar
from datetime import date, datetime

from parse_loadlog import (
    CFG, current_month_info,
    get_access_token, fetch_load_log_from_onedrive, parse_load_log,
)


class DateEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, date):
            return obj.isoformat()
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)


def main():
    print("=" * 50, file=sys.stderr)
    print("  Dashboard JSON Generator", file=sys.stderr)
    print("=" * 50, file=sys.stderr)

    if not os.environ.get('MS_GRAPH_REFRESH_TOKEN') or not os.environ.get('MS_GRAPH_CLIENT_ID'):
        print("ERROR: Graph API credentials not set", file=sys.stderr)
        sys.exit(1)

    print("\n[0] Auth...", file=sys.stderr)
    if not get_access_token():
        print("  FATAL: Could not authenticate", file=sys.stderr)
        sys.exit(1)

    mi = current_month_info()
    print(f"  Month: {mi['month_name']} {mi['year']}", file=sys.stderr)

    print("\n[1] Fetching load log...", file=sys.stderr)
    excel_bytes, filename, last_modified = fetch_load_log_from_onedrive()
    if not excel_bytes:
        print("  FATAL: No load log", file=sys.stderr)
        sys.exit(1)

    print("\n[2] Parsing...", file=sys.stderr)
    d = parse_load_log(excel_bytes, mi)

    # Prior month info
    prior = mi.get('prior_month', {})
    prior_key = mi.get('prior_month_key', '')
    prior_name = calendar.month_abbr[int(prior_key[-2:])] if prior_key else "Prior"

    # Build output
    output = {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "source_file": filename,
        "source_last_modified": last_modified.isoformat() if last_modified else None,
        "terminal": CFG.get('terminal', {}).get('name', 'Cadiz Terminal'),
        "company": CFG.get('terminal', {}).get('company', 'Timiron Midstream Partners'),
        "month": mi['month_name'],
        "month_abbr": mi['month_abbr'],
        "year": mi['year'],
        "days_in_month": mi['days_in_month'],

        "yesterday": {
            "date": d['yesterday_date'],
            "bbls": sum(v['bbls'] for v in d['pump_ute'].values()),
            "trucks": sum(v['loads'] for v in d['pump_ute'].values()),
            "splits": sum(v['splits'] for v in d['pump_ute'].values()),
        },

        "pump_utilization": d['pump_ute'],
        "pump_available_hrs": CFG.get('operations', {}).get('pump_available_hrs', 21),

        "mtd": {
            "total_bbls": d['total_bbls'],
            "total_trucks": d['total_trucks'],
            "days_actual": d['days_actual'],
            "days_remain": d['days_remain'],
            "avg_bbls": d['avg_bbls'],
            "avg_trucks": d['avg_trucks'],
            "rail_cap_pct": round(d['rail_cap'] * 100, 1),
        },

        "projection": {
            "proj_bbls": d['proj_bbls'],
            "proj_trucks": d['proj_trucks'],
            "proj_rev": d['proj_rev'],
            "ebitda": d['ebitda'],
        },

        "prior_month": {
            "name": prior_name,
            "total_bbls": prior.get('total_bbls', 0),
            "avg_bbls_per_day": prior.get('avg_bbls_per_day', 0),
        },

        "day_trend": d['day_trend'],
        "weeks": [{k: v for k, v in wk.items() if k != 'daily_detail'} for wk in d['weeks']],
        "carrier_actuals": d['carrier_actuals'],
        "carrier_rolling_avgs": d['carrier_rolling_avgs'],
        "wday_wkend": d['wday_wkend'],

        "config": {
            "pump_available_hrs": CFG.get('operations', {}).get('pump_available_hrs', 21),
            "rail_cap_daily_bbls": CFG.get('operations', {}).get('rail_cap_daily_bbls', 15000),
            "pumps": [p['name'] for p in CFG.get('operations', {}).get('pumps', [])],
            "carriers": [c['name'] for c in CFG.get('carriers', [])],
        },
    }

    # Output JSON to stdout
    print(json.dumps(output, cls=DateEncoder, indent=2))
    print("\nDONE", file=sys.stderr)


if __name__ == "__main__":
    main()
