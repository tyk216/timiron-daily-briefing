// Cadiz Ops Dashboard Worker
// Runs on Cloudflare's edge — no PC needed
// Cron: every 30 min pulls data from cadiz_ops OneDrive via Graph API
// Serves dashboard JSON and static files

const GRAPH_BASE = 'https://graph.microsoft.com/v1.0';
const TOKEN_URL = 'https://login.microsoftonline.com/common/oauth2/v2.0/token';
const SCOPE = 'offline_access Files.Read.All Sites.Read.All';
const EXCEL_EPOCH = new Date(1899, 11, 30); // Dec 30 1899

// ── Token Management ────────────────────────────────────────────
async function getToken(env) {
  const refreshToken = await env.KV.get('refresh_token');
  if (!refreshToken) throw new Error('No refresh token in KV');

  const r = await fetch(TOKEN_URL, {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: new URLSearchParams({
      client_id: env.CLIENT_ID,
      grant_type: 'refresh_token',
      refresh_token: refreshToken,
      scope: SCOPE,
    }),
  });

  const data = await r.json();
  if (!data.access_token) throw new Error(`Token refresh failed: ${data.error_description || JSON.stringify(data)}`);

  // Store new refresh token
  await env.KV.put('refresh_token', data.refresh_token);
  return data.access_token;
}

// ── Date Helpers ────────────────────────────────────────────────
function serialToDate(serial) {
  const ms = EXCEL_EPOCH.getTime() + serial * 86400000;
  return new Date(ms);
}

function dateToSerial(dt) {
  return Math.floor((dt.getTime() - EXCEL_EPOCH.getTime()) / 86400000);
}

function fmtDate(dt) {
  return dt.toISOString().split('T')[0];
}

// ── Find Load Log ──────────────────────────────────────────────
async function findLoadLog(token) {
  // Search for load log files — name format changes (e.g. "MASTER COPY - 1Q 2026", "Master Load Log 4.2.26 AFT")
  // Run two searches in parallel to catch both naming conventions
  const [r1, r2] = await Promise.all([
    fetch(`${GRAPH_BASE}/me/drive/root/search(q='MASTER COPY')`, { headers: { Authorization: `Bearer ${token}` } }),
    fetch(`${GRAPH_BASE}/me/drive/root/search(q='Master Load Log')`, { headers: { Authorization: `Bearer ${token}` } }),
  ]);

  const candidates = new Map(); // dedupe by item id
  for (const r of [r1, r2]) {
    if (!r.ok) continue;
    const data = await r.json();
    for (const item of (data.value || [])) {
      const name = item.name.toUpperCase();
      if (!name.endsWith('.XLSX')) continue;
      // Accept: "MASTER COPY - 1Q 2026...", "MASTER LOAD LOG..."
      const isLoadLog = (name.startsWith('MASTER COPY') && name.includes('LOAD LOG')) ||
                        name.startsWith('MASTER COPY - 1Q 2026') ||
                        name.startsWith('MASTER LOAD LOG');
      if (isLoadLog) candidates.set(item.id, item);
    }
  }

  // Pick the most recently modified file
  let best = null;
  for (const item of candidates.values()) {
    if (!best || item.lastModifiedDateTime > best.lastModifiedDateTime) {
      best = item;
    }
  }
  if (!best) throw new Error('No Master Load Log .xlsx file found on drive');
  return { id: best.id, name: best.name, modified: best.lastModifiedDateTime };
}

// ── Read Spreadsheet Data ──────────────────────────────────────
async function readData(token, fileId) {
  // Get current month boundaries (CST = UTC-6)
  const now = new Date();
  const cst = new Date(now.getTime() - 6 * 3600000);
  const monthStart = new Date(cst.getFullYear(), cst.getMonth(), 1);
  const startSerial = dateToSerial(monthStart);
  const todaySerial = dateToSerial(cst);

  const allRows = [];
  for (let cs = 2; cs < 6000; cs += 500) {
    const ce = cs + 499;
    const url = `${GRAPH_BASE}/me/drive/items/${fileId}/workbook/worksheets('Master_Load_Log')/range(address='A${cs}:X${ce}')`;
    const r = await fetch(url, { headers: { Authorization: `Bearer ${token}` } });
    if (!r.ok) continue;
    const data = await r.json();
    const rows = data.values || [];

    for (const row of rows) {
      if (row[1] && typeof row[1] === 'number' && row[1] >= startSerial && row[1] <= todaySerial + 1) {
        allRows.push(row);
      }
    }

    // Check if we've passed the date range
    const lastDates = rows.filter(r => r[1] && typeof r[1] === 'number').map(r => r[1]);
    if (lastDates.length && Math.max(...lastDates) > todaySerial + 5) break;
    if (!lastDates.length && cs > 4000) break;
  }

  return { rows: allRows, startSerial, todaySerial, now: cst };
}

// ── Calculate KPIs ─────────────────────────────────────────────
function calculateKPIs(rows, startSerial, todaySerial, now) {
  const daily = {};
  const getDay = (serial) => {
    if (!daily[serial]) {
      daily[serial] = {
        bbls: 0, trucks: 0, apiSum: 0, bswSum: 0, n: 0,
        pumpSum: 0, pumpN: 0, splits: 0,
        carriers: {}, pumps: {}
      };
    }
    return daily[serial];
  };

  for (const row of rows) {
    try {
      const day = Math.floor(row[1]);
      const carrier = (row[2] || 'Unknown').toString().trim();
      const bbls = parseFloat(row[17]) || 0;
      const api = parseFloat(row[13]) || 0;
      const bsw = parseFloat(row[14]) || 0;
      const split = (row[16] || '').toString().trim().toLowerCase();
      const pumpTime = parseFloat(row[22]) || 0;
      const bol = (row[23] || '').toString().trim();

      let pumpId = null;
      if (bol.startsWith('111')) pumpId = 'P-101';
      else if (bol.startsWith('222')) pumpId = 'P-102';
      else if (bol.startsWith('333')) pumpId = 'P-103';

      const d = getDay(day);
      d.bbls += bbls;

      const isSplit2 = split === 'split #2';
      if (!isSplit2) {
        d.trucks += 1;
        if (!d.carriers[carrier]) d.carriers[carrier] = { trucks: 0, bbls: 0 };
        d.carriers[carrier].trucks += 1;
      } else {
        d.splits += 1;
      }
      if (!d.carriers[carrier]) d.carriers[carrier] = { trucks: 0, bbls: 0 };
      d.carriers[carrier].bbls += bbls;

      if (pumpId) {
        if (!d.pumps[pumpId]) d.pumps[pumpId] = { loads: 0, splits: 0, runtime: 0, bbls: 0 };
        const p = d.pumps[pumpId];
        p.loads += 1;
        if (isSplit2) p.splits += 1;
        p.bbls += bbls;
        if (pumpTime > 0) p.runtime += pumpTime * 24;
      }

      if (api > 0) { d.apiSum += api; d.bswSum += bsw; d.n += 1; }
      if (pumpTime > 0) { d.pumpSum += pumpTime; d.pumpN += 1; }
    } catch (e) { continue; }
  }

  const sortedDays = Object.keys(daily).map(Number).sort((a, b) => a - b);
  if (!sortedDays.length) return null;

  const latest = sortedDays[sortedDays.length - 1];
  const t = daily[latest];
  const latestDate = serialToDate(latest);

  // Yesterday
  const prevDay = sortedDays.length >= 2 ? sortedDays[sortedDays.length - 2] : null;
  let yesterdayData = null;
  if (prevDay) {
    const pd = daily[prevDay];
    const prevDate = serialToDate(prevDay);
    yesterdayData = {
      date: fmtDate(prevDate),
      bbls: round(pd.bbls, 2),
      trucks: pd.trucks,
      splits: pd.splits,
      avg_api: pd.n > 0 ? round(pd.apiSum / pd.n, 2) : 0,
      avg_bsw: pd.n > 0 ? round(pd.bswSum / pd.n * 100, 2) : 0,
    };
  }

  // Today
  const todayData = {
    date: fmtDate(latestDate),
    bbls: round(t.bbls, 2),
    trucks: t.trucks,
    splits: t.splits,
    live: true,
  };

  // MTD — run rate uses only COMPLETED days (exclude today's partial data)
  const todaySerial2 = dateToSerial(now);
  let mtdBbls = 0, mtdTrucks = 0;
  let completedBbls = 0, completedTrucks = 0, completedDays = 0;
  for (const [dayKey, d] of Object.entries(daily)) {
    mtdBbls += d.bbls;
    mtdTrucks += d.trucks;
    if (Number(dayKey) !== todaySerial2) {
      completedBbls += d.bbls;
      completedTrucks += d.trucks;
      completedDays++;
    }
  }
  const daysActual = sortedDays.length;
  const daysInMonth = new Date(now.getFullYear(), now.getMonth() + 1, 0).getDate();
  const daysRemain = daysInMonth - daysActual;
  // Run rate based on completed days only
  const avgBbls = completedDays > 0 ? completedBbls / completedDays : 0;

  // Projection
  const projBbls = avgBbls * daysInMonth;
  const revPerBbl = 1.1032;
  const projRev = projBbls * revPerBbl;
  const fixedCost = 244583.5 / 12 * (daysInMonth / 30);

  // Weekly breakdown
  const weeks = [];
  let weekNum = 1;
  let weekData = { bbls: 0, trucks: 0, days: 0, start: null, end: null };
  for (const dk of sortedDays) {
    const dt = serialToDate(dk);
    if (dt.getDay() === 0 && weekData.days > 0) {
      const avg = weekData.days > 0 ? weekData.bbls / weekData.days : 0;
      const bpt = weekData.trucks > 0 ? weekData.bbls / weekData.trucks : 0;
      weeks.push({
        week_num: weekNum, start: weekData.start, end: weekData.end,
        total_bbls: Math.round(weekData.bbls), total_trucks: weekData.trucks,
        days: weekData.days, avg_bbls: round(avg, 1), bpt: round(bpt, 1),
      });
      weekNum++;
      weekData = { bbls: 0, trucks: 0, days: 0, start: null, end: null };
    }
    const dd = daily[dk];
    weekData.bbls += dd.bbls;
    weekData.trucks += dd.trucks;
    weekData.days += 1;
    if (!weekData.start) weekData.start = fmtDate(serialToDate(dk));
    weekData.end = fmtDate(serialToDate(dk));
  }
  if (weekData.days > 0) {
    const avg = weekData.bbls / weekData.days;
    const bpt = weekData.trucks > 0 ? weekData.bbls / weekData.trucks : 0;
    weeks.push({
      week_num: weekNum, start: weekData.start, end: weekData.end,
      total_bbls: Math.round(weekData.bbls), total_trucks: weekData.trucks,
      days: weekData.days, avg_bbls: round(avg, 1), bpt: round(bpt, 1),
    });
  }

  // 5-day trend
  const last5 = sortedDays.slice(-5);
  const trend = last5.map(dk => ({
    date: fmtDate(serialToDate(dk)),
    bbls: round(daily[dk].bbls, 2),
    trucks: daily[dk].trucks,
  }));

  // Carrier rolling averages
  const carrierRolling = {};
  const carrierToday = {};
  for (const [dk, dd] of Object.entries(daily)) {
    for (const [c, cv] of Object.entries(dd.carriers)) {
      if (!carrierRolling[c]) carrierRolling[c] = { totalTrucks: 0, totalBbls: 0 };
      carrierRolling[c].totalTrucks += cv.trucks;
      carrierRolling[c].totalBbls += cv.bbls;
      if (Number(dk) === latest) {
        carrierToday[c] = { trucks: cv.trucks, bbls: round(cv.bbls, 1) };
      }
    }
  }
  const carrierAvgs = {};
  for (const [c, cv] of Object.entries(carrierRolling)) {
    carrierAvgs[c] = {
      avg_bbls_per_truck: cv.totalTrucks > 0 ? round(cv.totalBbls / cv.totalTrucks, 1) : 0,
      avg_trucks_per_day: round(cv.totalTrucks / daysActual, 1),
      total_trucks: cv.totalTrucks,
      total_bbls: round(cv.totalBbls, 1),
    };
  }

  // Weekday vs weekend
  let wdayBbls = 0, wdayDays = 0, wdayTrucks = 0;
  let wkendBbls = 0, wkendDays = 0, wkendTrucks = 0;
  for (const dk of sortedDays) {
    const dt = serialToDate(dk);
    const dd = daily[dk];
    if (dt.getDay() >= 1 && dt.getDay() <= 5) {
      wdayBbls += dd.bbls; wdayTrucks += dd.trucks; wdayDays++;
    } else {
      wkendBbls += dd.bbls; wkendTrucks += dd.trucks; wkendDays++;
    }
  }

  // Pump utilization for today
  const pumpUtil = {};
  for (const [p, pv] of Object.entries(t.pumps)) {
    pumpUtil[p] = {
      loads: pv.loads, splits: pv.splits,
      runtime: round(pv.runtime, 2),
      ute: pv.runtime > 0 ? round(pv.runtime / 21 * 100, 1) : 0,
      bbls: Math.round(pv.bbls),
      bbl_hr: pv.runtime > 0 ? Math.round(pv.bbls / pv.runtime) : 0,
    };
  }

  const months = ['January','February','March','April','May','June','July','August','September','October','November','December'];
  const monthAbbrs = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];

  return {
    generated_at: new Date().toISOString(),
    source_file: 'MASTER COPY - FEB MASTER LOAD LOG (API)',
    source_last_modified: new Date().toISOString(),
    terminal: 'Cadiz Terminal',
    company: 'Timiron Midstream Partners',
    month: months[now.getMonth()],
    month_abbr: monthAbbrs[now.getMonth()],
    year: now.getFullYear(),
    days_in_month: daysInMonth,
    yesterday: todayData,
    yesterday_actual: yesterdayData,
    pump_utilization: pumpUtil,
    pump_available_hrs: 21,
    mtd: {
      total_bbls: round(completedBbls, 2), total_trucks: completedTrucks,
      days_actual: completedDays, days_remain: daysInMonth - completedDays,
      avg_bbls: round(avgBbls, 1),
      avg_trucks: completedDays > 0 ? round(completedTrucks / completedDays, 1) : 0,
      rail_cap_pct: round(avgBbls / 15000 * 100, 1),
      today_bbls: round(mtdBbls - completedBbls, 2),
      today_trucks: mtdTrucks - completedTrucks,
    },
    projection: {
      proj_bbls: Math.round(projBbls),
      proj_trucks: completedDays > 0 ? Math.round(completedTrucks / completedDays * daysInMonth) : 0,
      proj_rev: Math.round(projRev),
      ebitda: Math.round(projRev - fixedCost),
    },
    prior_month: { name: 'Feb', total_bbls: 313600, avg_bbls_per_day: 11200 },
    day_trend: trend,
    weeks,
    carrier_actuals: carrierToday,
    carrier_rolling_avgs: carrierAvgs,
    wday_wkend: {
      weekday: {
        days: wdayDays, total_bbls: round(wdayBbls, 1),
        avg_bbls: wdayDays > 0 ? round(wdayBbls / wdayDays, 1) : 0,
        total_trucks: wdayTrucks,
        avg_trucks: wdayDays > 0 ? round(wdayTrucks / wdayDays, 1) : 0,
      },
      weekend: {
        days: wkendDays, total_bbls: round(wkendBbls, 1),
        avg_bbls: wkendDays > 0 ? round(wkendBbls / wkendDays, 1) : 0,
        total_trucks: wkendTrucks,
        avg_trucks: wkendDays > 0 ? round(wkendTrucks / wkendDays, 1) : 0,
      },
    },
    config: {
      pump_available_hrs: 21,
      rail_cap_daily_bbls: 15000,
      pumps: ['P-101', 'P-102', 'P-103'],
      carriers: Object.keys(carrierAvgs),
    },
  };
}

function round(n, dec) {
  const f = Math.pow(10, dec);
  return Math.round(n * f) / f;
}

// ── QuickBooks Time — Crew Hours ───────────────────────────────
const QBT_API = 'https://rest.tsheets.com/api/v1';

const QBT_DAY_CREW = [
  'Cameron Betz', 'Shawn Osborn Jr.', 'Shane Young', 'William Glover',
  'Austin Tredway', 'Gregory Bates', 'Jared Wright', 'Shawn Osborn Sr.',
];
const QBT_NIGHT_CREW = [
  'Jonathan Williams', 'Daniel Hough', 'Bryan Deoss', 'Dustin Fletcher',
  'Jacob Diloreto', 'Nathaniel Medel', 'Christopher Wright',
];
const QBT_ROSTER = [...QBT_DAY_CREW, ...QBT_NIGHT_CREW];
const QBT_SHAWN_MAP = {
  'gosborn20@gmail.com': 'Shawn Osborn Jr.',
  'osbornshawn25@gmail.com': 'Shawn Osborn Sr.',
};
const QBT_ROLES = {
  'Cameron Betz': 'Manager', 'Shawn Osborn Jr.': 'Supervisor',
  'Jonathan Williams': 'Manager', 'Daniel Hough': 'Supervisor',
};

async function qbtGet(endpoint, params, qbtToken) {
  const all = {};
  let page = 1;
  while (true) {
    const p = new URLSearchParams({ ...params, page: String(page), per_page: '200' });
    const r = await fetch(`${QBT_API}/${endpoint}?${p}`, {
      headers: { 'Authorization': `Bearer ${qbtToken}` },
    });
    if (!r.ok) {
      const body = await r.text().catch(() => '');
      throw new Error(`QBT ${endpoint}: HTTP ${r.status} - ${body.slice(0, 200)}`);
    }
    let data;
    try { data = await r.json(); } catch { throw new Error(`QBT ${endpoint}: invalid JSON response`); }
    const results = (data.results || {})[endpoint] || {};
    if (!Object.keys(results).length) break;
    Object.assign(all, results);
    if (!data.more) break;
    page++;
  }
  return all;
}

async function refreshCrewHours(env) {
  const qbtToken = await env.KV.get('qbt_token');
  if (!qbtToken) return null;

  // User list cached in KV (changes rarely — only on hire/fire)
  // Refreshed once per day, or on first run
  let users = {};
  const cachedUsers = await env.KV.get('qbt_users');
  if (cachedUsers) {
    const parsed = JSON.parse(cachedUsers);
    const age = Date.now() - (parsed._ts || 0);
    if (age < 24 * 3600000) { // less than 24 hours old
      users = parsed.users;
    }
  }
  if (!Object.keys(users).length) {
    const rawUsers = await qbtGet('users', { active: 'yes' }, qbtToken);
    for (const [uid, u] of Object.entries(rawUsers)) {
      const first = (u.first_name || '').trim();
      const last = (u.last_name || '').trim();
      const email = (u.email || '').trim().toLowerCase();
      users[uid] = QBT_SHAWN_MAP[email] || `${first} ${last}`.trim();
    }
    await env.KV.put('qbt_users', JSON.stringify({ users, _ts: Date.now() }));
  }

  // Current week Mon-Sun in Eastern Time
  // Determine ET offset dynamically (EDT=-4, EST=-5) using US DST rules:
  // DST starts 2nd Sunday of March, ends 1st Sunday of November
  const nowMs = Date.now();
  const nowUTC = new Date(nowMs);
  const year = nowUTC.getUTCFullYear();
  const mar1 = new Date(Date.UTC(year, 2, 1));
  const dstStart = new Date(Date.UTC(year, 2, 14 - mar1.getUTCDay(), 7)); // 2nd Sun Mar, 2AM EST = 7AM UTC
  const nov1 = new Date(Date.UTC(year, 10, 1));
  const dstEnd = new Date(Date.UTC(year, 10, 7 - nov1.getUTCDay(), 6));   // 1st Sun Nov, 2AM EDT = 6AM UTC
  const isDST = nowMs >= dstStart.getTime() && nowMs < dstEnd.getTime();
  const etOffHours = isDST ? -4 : -5;
  const etOff = etOffHours * 3600000;

  // Calculate today in ET as a YYYY-MM-DD string (avoid Date constructor timezone issues)
  const etMs = nowMs + etOff;
  const todayStr = new Date(etMs).toISOString().split('T')[0]; // safe: offset already applied
  const todayParts = todayStr.split('-').map(Number);
  // Day of week: 0=Sun..6=Sat -> convert to Mon=1..Sun=7
  const tmpDate = new Date(Date.UTC(todayParts[0], todayParts[1]-1, todayParts[2]));
  const dow = tmpDate.getUTCDay() || 7;
  const mondayDate = new Date(Date.UTC(todayParts[0], todayParts[1]-1, todayParts[2] - (dow - 1)));
  const sundayDate = new Date(mondayDate.getTime() + 6 * 86400000);
  const yesterdayDate = new Date(mondayDate.getTime() - 86400000);

  const fmtD = d => d.toISOString().split('T')[0];

  // Fetch completed + active timesheets
  const completed = await qbtGet('timesheets', { start_date: fmtD(mondayDate), end_date: fmtD(sundayDate) }, qbtToken);
  const activeY = await qbtGet('timesheets', { on_the_clock: 'yes', start_date: fmtD(yesterdayDate) }, qbtToken);
  const activeT = await qbtGet('timesheets', { on_the_clock: 'yes', start_date: fmtD(mondayDate) }, qbtToken);
  const all = { ...completed, ...activeY, ...activeT };

  // Aggregate + track who's currently on the clock
  const emp = {};
  const onClock = new Set();
  for (const ts of Object.values(all)) {
    const name = users[String(ts.user_id)] || `Unknown (${ts.user_id})`;
    let dur = ts.duration || 0;
    if (dur === 0 && !ts.end) {
      const start = new Date(ts.start);
      dur = Math.floor((nowMs - start.getTime()) / 1000);
      onClock.add(name);
    }
    emp[name] = (emp[name] || 0) + dur;
  }

  // Build sorted rows
  const rows = [];
  for (const name of QBT_ROSTER) {
    if (!(name in emp)) continue;
    const total = round(emp[name] / 3600, 1);
    const shift = QBT_DAY_CREW.includes(name) ? 'Day' : 'Night';
    const role = QBT_ROLES[name] || '';
    rows.push({ name, shift, role, total, reg: Math.min(total, 40), ot: round(Math.max(total - 40, 0), 1), on_clock: onClock.has(name) });
  }
  // Anyone not on roster
  for (const [name, secs] of Object.entries(emp)) {
    if (QBT_ROSTER.includes(name)) continue;
    const total = round(secs / 3600, 1);
    rows.push({ name, shift: '-', role: '', total, reg: Math.min(total, 40), ot: round(Math.max(total - 40, 0), 1), on_clock: onClock.has(name) });
  }

  const dayRows = rows.filter(r => r.shift === 'Day');
  const nightRows = rows.filter(r => r.shift === 'Night');
  const months = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];

  const totalHrs = round(rows.reduce((s, r) => s + r.total, 0), 1);

  // Monthly efficiency history (QBT hours from actual data, BBLs from dashboard)
  // Pre-computed from historical data to avoid extra API calls each refresh
  const monthlyEfficiency = [
    { month: 'Sep 25', hrs: 1333, bbls: 241480, hc: 12 },
    { month: 'Oct 25', hrs: 2715, bbls: 220569, hc: 12 },
    { month: 'Nov 25', hrs: 2227, bbls: 176502, hc: 12 },
    { month: 'Dec 25', hrs: 2300, bbls: 195816, hc: 12 },
    { month: 'Jan 26', hrs: 2752, bbls: 308000, hc: 12 },
    { month: 'Feb 26', hrs: 3313, bbls: 313600, hc: 13 },
    { month: 'Mar 26', hrs: 3701, bbls: 319681, hc: 15 },
  ].map(m => ({
    ...m,
    bbls_per_hr: round(m.bbls / m.hrs, 1),
    labor_per_bbl: round(m.hrs * 20 / m.bbls, 2),
  }));

  // Pull WTD BBLs from dashboard data (same week as crew hours) for efficiency metric
  let bbls_per_labor_hr = null;
  let wtd_bbls = null;
  try {
    const dashJson = await env.KV.get('dashboard_json');
    if (dashJson) {
      const dash = JSON.parse(dashJson);
      // Sum completed days from trend that fall within this week (Mon-Sun)
      const mondayStr = fmtD(mondayDate);
      const todayStr2 = `${todayParts[0]}-${String(todayParts[1]).padStart(2,'0')}-${String(todayParts[2]).padStart(2,'0')}`;
      const trend = dash.day_trend || [];
      let weekBbls = 0;
      for (const t of trend) {
        if (t.date >= mondayStr && t.date < todayStr2) {
          weekBbls += t.bbls;
        }
      }
      wtd_bbls = round(weekBbls, 1);
      if (weekBbls > 0 && totalHrs > 0) {
        bbls_per_labor_hr = round(weekBbls / totalHrs, 1);
      }
    }
  } catch(e) { /* ignore */ }

  const crew = {
    rows,
    week_label: `${months[mondayDate.getUTCMonth()]} ${mondayDate.getUTCDate()}-${months[todayParts[1]-1]} ${todayParts[2]}`,
    total_hrs: totalHrs,
    total_ot: round(rows.reduce((s, r) => s + r.ot, 0), 1),
    day_count: dayRows.length,
    night_count: nightRows.length,
    day_avg: dayRows.length ? round(dayRows.reduce((s,r) => s+r.total, 0) / dayRows.length, 1) : 0,
    night_avg: nightRows.length ? round(nightRows.reduce((s,r) => s+r.total, 0) / nightRows.length, 1) : 0,
    bbls_per_labor_hr,
    wtd_bbls,
    monthly_efficiency: monthlyEfficiency,
    generated_at: new Date().toISOString(),
  };

  await env.KV.put('crew_json', JSON.stringify(crew));
  return crew;
}

// ── Refresh Logic ──────────────────────────────────────────────
async function refreshDashboard(env) {
  const token = await getToken(env);
  const file = await findLoadLog(token);
  const { rows, startSerial, todaySerial, now } = await readData(token, file.id);
  const dashboard = calculateKPIs(rows, startSerial, todaySerial, now);
  if (!dashboard) throw new Error('No data found');

  dashboard.source_file = file.name;
  dashboard.source_last_modified = file.modified;

  await env.KV.put('dashboard_json', JSON.stringify(dashboard));
  await env.KV.put('last_refresh', new Date().toISOString());

  // Refresh crew hours every 60 min (non-blocking, don't fail dashboard if QBT fails)
  try {
    const lastCrew = await env.KV.get('crew_last_refresh');
    const crewAge = lastCrew ? Date.now() - new Date(lastCrew).getTime() : Infinity;
    if (crewAge >= 60 * 60 * 1000) {
      await refreshCrewHours(env);
      await env.KV.put('crew_last_refresh', new Date().toISOString());
    }
  } catch (e) { console.error('Crew refresh failed:', e.message); }

  return dashboard;
}

// ── Worker Entry Points ────────────────────────────────────────
export default {
  // HTTP handler
  async fetch(request, env) {
    const url = new URL(request.url);
    const corsHeaders = {
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
      'Access-Control-Allow-Headers': 'Content-Type',
    };

    if (request.method === 'OPTIONS') {
      return new Response(null, { headers: corsHeaders });
    }

    // GET /api/dashboard — serve cached JSON
    if (url.pathname === '/api/dashboard' || url.pathname === '/api/dashboard/') {
      const json = await env.KV.get('dashboard_json');
      if (!json) return new Response('{"error":"No data yet"}', {
        status: 503, headers: { ...corsHeaders, 'Content-Type': 'application/json' }
      });
      return new Response(json, {
        headers: { ...corsHeaders, 'Content-Type': 'application/json', 'Cache-Control': 'public, max-age=60' }
      });
    }

    // POST /api/refresh — trigger immediate refresh (bypasses 60-min crew throttle)
    if (url.pathname === '/api/refresh' && request.method === 'POST') {
      try {
        const dashboard = await refreshDashboard(env);
        // Force crew refresh on manual request
        try { await refreshCrewHours(env); await env.KV.put('crew_last_refresh', new Date().toISOString()); } catch(e) { console.error('Crew:', e.message); }
        return new Response(JSON.stringify({
          ok: true,
          bbls: dashboard.yesterday.bbls,
          trucks: dashboard.yesterday.trucks,
          generated_at: dashboard.generated_at,
        }), { headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
      } catch (e) {
        return new Response(JSON.stringify({ ok: false, error: e.message }), {
          status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' }
        });
      }
    }

    // GET /api/crew — crew hours WTD
    if (url.pathname === '/api/crew' || url.pathname === '/api/crew/') {
      const json = await env.KV.get('crew_json');
      if (!json) return new Response('{"error":"No crew data yet"}', {
        status: 503, headers: { ...corsHeaders, 'Content-Type': 'application/json' }
      });
      return new Response(json, {
        headers: { ...corsHeaders, 'Content-Type': 'application/json', 'Cache-Control': 'public, max-age=60' }
      });
    }

    // GET /api/status — last refresh time
    if (url.pathname === '/api/status') {
      const lastRefresh = await env.KV.get('last_refresh');
      return new Response(JSON.stringify({ last_refresh: lastRefresh }), {
        headers: { ...corsHeaders, 'Content-Type': 'application/json' }
      });
    }

    return new Response('Not found', { status: 404, headers: corsHeaders });
  },

  // Cron handler — runs every 30 min
  async scheduled(event, env, ctx) {
    ctx.waitUntil(refreshDashboard(env));
  },
};
