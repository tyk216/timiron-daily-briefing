// Cadiz Ops Dashboard Worker
// Runs on Cloudflare's edge — no PC needed
// Cron: every 30 min pulls data from cadiz_ops OneDrive via Graph API
// Serves dashboard JSON and static files

const GRAPH_BASE = 'https://graph.microsoft.com/v1.0';
const TOKEN_URL = 'https://login.microsoftonline.com/132a8676-8518-49e8-885a-ea8d5ec0a533/oauth2/v2.0/token';
// Re-consented 2026-05-05 — refresh_token now carries Files.ReadWrite.All + Mail.Send.
// See: _staging/flagman_oauth_reconsent_2026-05-05_report.md
const SCOPE = 'offline_access Files.ReadWrite.All Sites.Read.All Mail.Send';
const SCOPE_WRITE = SCOPE;
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
      client_secret: env.ATLAS_CLIENT_SECRET,
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

// Write-scope token. Uses same refresh_token; scope-upgrades on refresh.
// Cached briefly in KV (50 min) to avoid repeated refresh calls per request.
async function getWriteToken(env) {
  const cached = await env.KV.get('graph_write_token');
  if (cached) return cached;

  const refreshToken = await env.KV.get('refresh_token');
  if (!refreshToken) throw new Error('No refresh token in KV');

  const r = await fetch(TOKEN_URL, {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: new URLSearchParams({
      client_id: env.CLIENT_ID,
      client_secret: env.ATLAS_CLIENT_SECRET,
      grant_type: 'refresh_token',
      refresh_token: refreshToken,
      scope: SCOPE_WRITE,
    }),
  });

  const data = await r.json();
  if (!data.access_token) {
    throw new Error(`Write-scope token refresh failed: ${data.error_description || JSON.stringify(data)}`);
  }
  await env.KV.put('refresh_token', data.refresh_token);
  await env.KV.put('graph_write_token', data.access_token, { expirationTtl: 3000 });
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

// ── Find Load Log (resilient: pinned ID + broad search + alerts) ──
async function findLoadLog(token, env) {
  // Step 1: Try pinned file ID first (survives moves/renames)
  const pinnedId = await env.KV.get('load_log_file_id');
  if (pinnedId) {
    try {
      const r = await fetch(`${GRAPH_BASE}/me/drive/items/${pinnedId}`, {
        headers: { Authorization: `Bearer ${token}` },
      });
      if (r.ok) {
        const item = await r.json();
        // Verify it's been modified in the last 48 hours (not a stale ghost)
        const modAge = Date.now() - new Date(item.lastModifiedDateTime).getTime();
        if (modAge < 48 * 3600000) {
          const prevName = await env.KV.get('load_log_file_name');
          const prevPath = await env.KV.get('load_log_file_path');
          const curPath = item.parentReference?.path || '';
          // Detect path/name changes and log them
          if (prevName && prevName !== item.name) {
            await env.KV.put('load_log_alert', JSON.stringify({
              level: 'WARNING', time: new Date().toISOString(),
              msg: `Master Load Log renamed: "${prevName}" → "${item.name}"`,
            }));
          }
          if (prevPath && prevPath !== curPath) {
            await env.KV.put('load_log_alert', JSON.stringify({
              level: 'WARNING', time: new Date().toISOString(),
              msg: `Master Load Log moved: ${prevPath} → ${curPath}`,
            }));
          }
          // Update pinned metadata for rename/move detection, but DO NOT
          // early-return — a new Master Load Log file is created every night
          // between 1–5 AM ET with a new filename date. We must always run the
          // broad search below and pick the newest by lastModifiedDateTime.
          await env.KV.put('load_log_file_name', item.name);
          await env.KV.put('load_log_file_path', curPath);
        }
      }
    } catch (e) { /* pinned ID failed, fall through to search */ }
  }

  // Step 2: Broad search — three parallel queries
  const [r1, r2, r3] = await Promise.all([
    fetch(`${GRAPH_BASE}/me/drive/root/search(q='MASTER COPY')`, { headers: { Authorization: `Bearer ${token}` } }),
    fetch(`${GRAPH_BASE}/me/drive/root/search(q='Master Load Log')`, { headers: { Authorization: `Bearer ${token}` } }),
    fetch(`${GRAPH_BASE}/me/drive/root/search(q='load log')`, { headers: { Authorization: `Bearer ${token}` } }),
  ]);

  const candidates = new Map();
  for (const r of [r1, r2, r3]) {
    if (!r.ok) continue;
    const data = await r.json();
    for (const item of (data.value || [])) {
      const name = item.name.toUpperCase();
      if (!name.endsWith('.XLSX')) continue;
      // Broad match: any xlsx with "LOAD" and "LOG" in the name
      const isLoadLog = (name.includes('LOAD') && name.includes('LOG')) ||
                        (name.startsWith('MASTER COPY') && name.includes('LOG')) ||
                        name.startsWith('MASTER LOAD LOG');
      if (isLoadLog) candidates.set(item.id, item);
    }
  }

  if (candidates.size === 0) {
    // CRITICAL: No file found — write alert
    await env.KV.put('load_log_alert', JSON.stringify({
      level: 'CRITICAL', time: new Date().toISOString(),
      msg: 'No Master Load Log found on Cadiz OneDrive. Dashboard data is STALE.',
    }));
    throw new Error('No Master Load Log .xlsx file found on drive');
  }

  // Step 3: Pick best candidate — prefer recently modified, then largest
  const now = Date.now();
  let best = null;
  for (const item of candidates.values()) {
    const age = now - new Date(item.lastModifiedDateTime).getTime();
    const isRecent = age < 48 * 3600000;
    if (!best) { best = item; continue; }
    const bestAge = now - new Date(best.lastModifiedDateTime).getTime();
    const bestRecent = bestAge < 48 * 3600000;
    // Prefer recent over old
    if (isRecent && !bestRecent) { best = item; continue; }
    if (!isRecent && bestRecent) continue;
    // Among same recency tier, prefer most recently modified
    if (item.lastModifiedDateTime > best.lastModifiedDateTime) best = item;
  }

  // Log rollover only when the pinned file actually changed (e.g. nightly
  // file rotation), not on every request.
  if (pinnedId && best.id !== pinnedId) {
    await env.KV.put('load_log_alert', JSON.stringify({
      level: 'INFO', time: new Date().toISOString(),
      msg: `Master Load Log rolled over to: "${best.name}"`,
    }));
  }

  // Pin the found file for next run
  await env.KV.put('load_log_file_id', best.id);
  await env.KV.put('load_log_file_name', best.name);
  await env.KV.put('load_log_file_path', best.parentReference?.path || '');
  await env.KV.put('load_log_pinned_at', new Date().toISOString());

  // Log selection for debugging
  await env.KV.put('load_log_selection_log', JSON.stringify({
    time: new Date().toISOString(),
    selected: best.name,
    candidates: [...candidates.values()].map(c => ({ name: c.name, modified: c.lastModifiedDateTime })),
  }));

  return { id: best.id, name: best.name, modified: best.lastModifiedDateTime };
}

// ── Read Spreadsheet Data ──────────────────────────────────────
function getEastern(dt) {
  const str = dt.toLocaleString('en-US', { timeZone: 'America/New_York' });
  return new Date(str);
}

async function readData(token, fileId) {
  // Get current month boundaries (Eastern time — handles DST automatically)
  const now = new Date();
  const eastern = getEastern(now);
  const monthStart = new Date(eastern.getFullYear(), eastern.getMonth(), 1);
  const startSerial = dateToSerial(monthStart);
  const todaySerial = dateToSerial(eastern);

  const allRows = [];
  for (let cs = 2; cs < 15000; cs += 500) {
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

  return { rows: allRows, startSerial, todaySerial, now: eastern };
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

// ── Daily Crew (Corporate App) ────────────────────────────────
async function refreshDailyCrew(env) {
  const qbtToken = await env.KV.get('qbt_token');
  if (!qbtToken) return null;

  // Resolve users (reuse cached list from refreshCrewHours)
  let users = {};
  const cachedUsers = await env.KV.get('qbt_users');
  if (cachedUsers) {
    const parsed = JSON.parse(cachedUsers);
    users = parsed.users || {};
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

  // ET offset (same DST logic as refreshCrewHours)
  const nowMs = Date.now();
  const nowUTC = new Date(nowMs);
  const year = nowUTC.getUTCFullYear();
  const mar1 = new Date(Date.UTC(year, 2, 1));
  const dstStart = new Date(Date.UTC(year, 2, 14 - mar1.getUTCDay(), 7));
  const nov1 = new Date(Date.UTC(year, 10, 1));
  const dstEnd = new Date(Date.UTC(year, 10, 7 - nov1.getUTCDay(), 6));
  const isDST = nowMs >= dstStart.getTime() && nowMs < dstEnd.getTime();
  const etOffHours = isDST ? -4 : -5;
  const etOff = etOffHours * 3600000;

  const etMs = nowMs + etOff;
  const todayStr = new Date(etMs).toISOString().split('T')[0];
  const todayParts = todayStr.split('-').map(Number);
  const yesterdayD = new Date(Date.UTC(todayParts[0], todayParts[1]-1, todayParts[2] - 1));
  const dayBeforeD = new Date(Date.UTC(todayParts[0], todayParts[1]-1, todayParts[2] - 2));
  const fmtD = d => d.toISOString().split('T')[0];
  const yesterdayStr = fmtD(yesterdayD);
  const dayBeforeStr = fmtD(dayBeforeD);

  // Fetch timesheets for 3 days + active
  const [tsToday, tsYesterday, tsDayBefore, tsActive] = await Promise.all([
    qbtGet('timesheets', { start_date: todayStr, end_date: todayStr }, qbtToken),
    qbtGet('timesheets', { start_date: yesterdayStr, end_date: yesterdayStr }, qbtToken),
    qbtGet('timesheets', { start_date: dayBeforeStr, end_date: dayBeforeStr }, qbtToken),
    qbtGet('timesheets', { on_the_clock: 'yes', start_date: dayBeforeStr }, qbtToken),
  ]);
  const allTs = { ...tsDayBefore, ...tsYesterday, ...tsToday, ...tsActive };

  // Parse individual entries
  const entries = [];
  const seen = new Set();
  for (const ts of Object.values(allTs)) {
    const name = users[String(ts.user_id)] || `Unknown (${ts.user_id})`;
    const startRaw = ts.start;
    if (!startRaw) continue;

    const startDate = new Date(startRaw);
    const dedupKey = `${name}|${startDate.getTime()}`;
    if (seen.has(dedupKey)) continue;
    seen.add(dedupKey);

    const endRaw = ts.end;
    const endDate = endRaw ? new Date(endRaw) : null;
    const isOnClock = !endDate && (ts.duration === 0 || !ts.duration);

    let hours = 0;
    if (endDate) {
      hours = round((endDate.getTime() - startDate.getTime()) / 3600000, 1);
    } else if (isOnClock) {
      hours = round((nowMs - startDate.getTime()) / 3600000, 1);
    }

    // Convert start time to ET for classification
    const startET = new Date(startDate.getTime() + etOff);
    const startHour = startET.getUTCHours();
    const startDateStr = startET.toISOString().split('T')[0];

    // Classify shift: roster first, then by time
    let shift;
    if (QBT_DAY_CREW.includes(name)) {
      shift = 'Day';
    } else if (QBT_NIGHT_CREW.includes(name)) {
      shift = 'Night';
    } else {
      shift = startHour >= 16 || startHour < 4 ? 'Night' : 'Day';
    }

    // Format times in ET
    const fmtTime = (d) => {
      if (!d) return null;
      const et = new Date(d.getTime() + etOff);
      let h = et.getUTCHours();
      const m = et.getUTCMinutes();
      const ampm = h >= 12 ? 'PM' : 'AM';
      h = h % 12 || 12;
      return `${h}:${String(m).padStart(2, '0')} ${ampm}`;
    };

    // Night shift starting on Day N evening → belongs to Day N+1's report
    // EXCEPT: if still on clock, show on today (they're working RIGHT NOW)
    let reportDate;
    if (shift === 'Night' && startHour >= 16 && !isOnClock) {
      const nextDay = new Date(startET.getTime() + 86400000);
      reportDate = nextDay.toISOString().split('T')[0];
    } else if (isOnClock) {
      reportDate = todayStr; // active entries always show on today
    } else {
      reportDate = startDateStr;
    }

    // Detect flags
    const flags = [];
    if (hours > 0 && hours < 4 && !isOnClock) {
      flags.push({ type: 'short_shift', severity: 'warning', message: `${hours} hours — unusually short shift` });
    }
    if (hours > 16) {
      flags.push({ type: 'extreme_ot', severity: 'error', message: `${hours} hours — verify this entry` });
    } else if (hours > 14) {
      flags.push({ type: 'overtime', severity: 'warning', message: `${hours} hours — exceeds 14-hour threshold` });
    }
    if (isOnClock && hours > 14) {
      flags.push({ type: 'missing_clockout', severity: 'error', message: `Still clocked in — ${hours} hours elapsed` });
    }
    if (!endDate && !isOnClock) {
      flags.push({ type: 'no_end', severity: 'error', message: 'Missing clock-out — no end time recorded' });
    }
    // Late arrival: Day shift expected ~5:30-6:30 AM, Night shift ~5:00-6:00 PM
    if (shift === 'Day' && startHour >= 7 && !isOnClock) {
      flags.push({ type: 'late', severity: 'info', message: `Clocked in at ${fmtTime(startDate)} — later than usual` });
    }
    if (shift === 'Night' && startHour >= 18 && startHour < 22 && !isOnClock) {
      flags.push({ type: 'late', severity: 'info', message: `Clocked in at ${fmtTime(startDate)} — later than usual` });
    }
    // Early departure: < 10 hours for a completed shift (normal is ~12)
    if (!isOnClock && endDate && hours >= 4 && hours < 10) {
      flags.push({ type: 'early_out', severity: 'info', message: `${hours} hours — left earlier than usual` });
    }

    entries.push({
      name, shift, clock_in: fmtTime(startDate), clock_out: fmtTime(endDate),
      hours, on_clock: isOnClock, flags, reportDate,
      _startMs: startDate.getTime(),
      _endMs: endDate ? endDate.getTime() : nowMs,
    });
  }

  // Merge midnight-split entries: QB Time breaks night shifts at 12:00 AM
  // into two entries. Merge by temporal proximity (gap < 2 hours = same shift).
  const byEmployee2 = {};
  for (const e of entries) {
    if (!byEmployee2[e.name]) byEmployee2[e.name] = [];
    byEmployee2[e.name].push(e);
  }

  const merged = [];
  for (const empEntries of Object.values(byEmployee2)) {
    empEntries.sort((a, b) => a._startMs - b._startMs);
    let i = 0;
    while (i < empEntries.length) {
      const group = [empEntries[i]];
      // Absorb consecutive entries within 2-hour gap (use actual end time, not rounded hours)
      while (i + 1 < empEntries.length) {
        const curr = group[group.length - 1];
        const next = empEntries[i + 1];
        const gap = (next._startMs - curr._endMs) / 3600000; // hours
        if (gap >= -0.1 && gap < 2 && curr.shift === next.shift) {
          group.push(next);
          i++;
        } else {
          break;
        }
      }
      i++;

      if (group.length === 1) {
        merged.push(group[0]);
        continue;
      }

      // Merge group: earliest clock_in, latest clock_out, sum hours
      const first = group[0];
      const last = group[group.length - 1];
      const totalHours = round(group.reduce((s, e) => s + e.hours, 0), 1);
      const anyOnClock = group.some(e => e.on_clock);
      const allFlags = group.flatMap(e => e.flags).filter(f => f.type !== 'short_shift');
      if (totalHours > 16) {
        allFlags.push({ type: 'extreme_ot', severity: 'error', message: `${totalHours} hours — verify this entry` });
      } else if (totalHours > 14) {
        allFlags.push({ type: 'overtime', severity: 'warning', message: `${totalHours} hours — exceeds 14-hour threshold` });
      }

      merged.push({
        name: first.name,
        shift: first.shift,
        clock_in: first.clock_in,
        clock_out: last.clock_out,
        hours: totalHours,
        on_clock: anyOnClock,
        flags: allFlags,
        reportDate: first.reportDate,
        _startMs: first._startMs,
        _endMs: last._endMs,
      });
    }
  }

  // Replace entries with merged list
  entries.length = 0;
  entries.push(...merged);

  // Check for double shifts (multiple merged entries on same day for same employee)
  const byEmpDay = {};
  for (const e of entries) {
    const k = `${e.name}|${e.reportDate}`;
    if (!byEmpDay[k]) byEmpDay[k] = [];
    byEmpDay[k].push(e);
  }
  for (const group of Object.values(byEmpDay)) {
    if (group.length < 2) continue;
    const totalHrs = round(group.reduce((s, e) => s + e.hours, 0), 1);
    if (totalHrs > 16) {
      group[group.length - 1].flags.push({ type: 'double_shift', severity: 'warning', message: `${totalHrs} combined hours today — verify` });
    }
  }

  // Build day report helper
  function buildDay(dateStr) {
    const dayEntries = entries.filter(e => e.reportDate === dateStr);
    const dayShift = dayEntries.filter(e => e.shift === 'Day').sort((a,b) => a._startMs - b._startMs);
    const nightShift = dayEntries.filter(e => e.shift === 'Night').sort((a,b) => a._startMs - b._startMs);
    const working = new Set(dayEntries.map(e => e.name));
    const off = QBT_ROSTER.filter(n => !working.has(n));
    const allFlags = dayEntries.flatMap(e => e.flags.map(f => ({ employee: e.name, ...f })));

    if (dayEntries.length > 0 && dayEntries.length < 4) {
      allFlags.push({ employee: '', type: 'light_staffing', severity: 'info', message: `Light staffing day — ${dayEntries.length} crew total` });
    }

    const clean = (arr) => arr.map(({ _startMs, reportDate, ...rest }) => rest);
    const dayHrs = round(dayShift.reduce((s,e) => s + e.hours, 0), 1);
    const nightHrs = round(nightShift.reduce((s,e) => s + e.hours, 0), 1);

    const dt = new Date(dateStr + 'T12:00:00');
    const dayNames = ['Sun','Mon','Tue','Wed','Thu','Fri','Sat'];
    const monthNames = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
    const display = `${dayNames[dt.getDay()]}, ${monthNames[dt.getMonth()]} ${dt.getDate()}`;

    return {
      date: dateStr,
      display,
      summary: {
        on_shift: dayEntries.length, off: off.length,
        day_hours: dayHrs, night_hours: nightHrs,
        total_hours: round(dayHrs + nightHrs, 1),
        day_count: dayShift.length, night_count: nightShift.length,
      },
      day_shift: clean(dayShift),
      night_shift: clean(nightShift),
      off,
      flags: allFlags,
    };
  }

  const result = {
    generated_at: new Date().toISOString(),
    today: buildDay(todayStr),
    yesterday: buildDay(yesterdayStr),
    roster: { total: QBT_ROSTER.length, members: QBT_ROSTER },
    data_source: {
      system: 'QuickBooks Time',
      refresh: 'Live — includes active shifts',
      night_note: 'Night shifts span midnight — clocked in evening, out next morning',
    },
  };

  await env.KV.put(`daily-crew-${todayStr}`, JSON.stringify(result), { expirationTtl: 300 });
  return result;
}

// ── Refresh Logic ──────────────────────────────────────────────
async function refreshDashboard(env) {
  const token = await getToken(env);
  const file = await findLoadLog(token, env);
  const { rows, startSerial, todaySerial, now } = await readData(token, file.id);
  const dashboard = calculateKPIs(rows, startSerial, todaySerial, now);
  if (!dashboard) throw new Error('No data found');

  dashboard.source_file = file.name;
  dashboard.source_last_modified = file.modified;

  // Stale data detection — compare KPI hash with previous run
  const prevHash = await env.KV.get('dashboard_kpi_hash');
  const curHash = `${dashboard.mtd?.total_trucks || 0}-${dashboard.mtd?.total_bbls || 0}`;
  if (prevHash && prevHash === curHash) {
    const staleCount = parseInt(await env.KV.get('dashboard_stale_count') || '0') + 1;
    await env.KV.put('dashboard_stale_count', String(staleCount));
    if (staleCount >= 2) {
      await env.KV.put('load_log_alert', JSON.stringify({
        level: 'WARNING', time: new Date().toISOString(),
        msg: `Dashboard KPIs unchanged for ${staleCount} consecutive cycles (~${staleCount * 30} min). Possible stale data.`,
      }));
    }
  } else {
    await env.KV.put('dashboard_stale_count', '0');
  }
  await env.KV.put('dashboard_kpi_hash', curHash);

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

// ── Railcar Capacity Tracker ──────────────────────────────────
async function findRailcarTracker(token) {
  const r = await fetch(`${GRAPH_BASE}/me/drive/root/search(q='Railcar Capacity Tracker')`, {
    headers: { Authorization: `Bearer ${token}` },
  });
  if (!r.ok) throw new Error('Search failed for Railcar Capacity Tracker');
  const data = await r.json();
  let best = null;
  for (const item of (data.value || [])) {
    const name = item.name.toUpperCase();
    if (!name.endsWith('.XLSX')) continue;
    if (name.includes('RAILCAR CAPACITY TRACKER') || name.includes('RAILCAR CAPACITY')) {
      if (!best || item.lastModifiedDateTime > best.lastModifiedDateTime) best = item;
    }
  }
  if (!best) throw new Error('No Railcar Capacity Tracker found on drive');
  return { id: best.id, name: best.name, modified: best.lastModifiedDateTime };
}

async function readRailcarCapacity(token, fileId) {
  // Step 1: List all worksheets to find the right one
  const wsUrl = `${GRAPH_BASE}/me/drive/items/${fileId}/workbook/worksheets`;
  const wsResp = await fetch(wsUrl, { headers: { Authorization: `Bearer ${token}` } });
  let sheetNames = [];
  if (wsResp.ok) {
    const wsData = await wsResp.json();
    sheetNames = (wsData.value || []).map(s => s.name);
  }

  let rows = null;
  let usedSheet = null;

  // Try each sheet with usedRange to get all data
  for (const sheet of (sheetNames.length > 0 ? sheetNames : ['Sheet1'])) {
    const url = `${GRAPH_BASE}/me/drive/items/${fileId}/workbook/worksheets('${encodeURIComponent(sheet)}')/usedRange`;
    const r = await fetch(url, { headers: { Authorization: `Bearer ${token}` } });
    if (r.ok) {
      const data = await r.json();
      rows = data.values || [];
      usedSheet = sheet;
      if (rows.length > 0) break;
    }
  }

  if (!rows || rows.length === 0) {
    return { startingCapacity: null, totalLoaded: null, remainingCapacity: null, _error: `No data. Sheets: [${sheetNames.join(', ')}]. usedSheet: ${usedSheet}` };
  }

  return parseRailcarData(rows);
}

// Parser columns for MASTER COPY - Railcar Capacity Tracker MM.DD.YY SOD.xlsx
// Column layout verified from live xlsx 2026-04-14:
//   [1] = row number within section    [2] = railcar ID (e.g., "NATX 31806")
//   [4] = R/C Max Cap bbl               [7] = R/C Safe Cap bbl
//   [8..13] = Load 1..6 (gallons per pump run)
//   [15] = Total loaded bbl             [17] = R/C Remaining bbl (per-car)
//   [9] = section header column (Load Track / Rear Track / Derail Track)
//   [23] = X column — aggregate BBL values paired with label text elsewhere in row
// Section header rows: R02 col 9="Load Track", R28 col 9="Rear Track", R37 col 9="Derail Track".
// State detection (empirically verified against pump count):
//   state='loading' if loaded > 0 AND remaining > 5  (→ a pump is actively on it)
//   state='loaded'  if loaded > 0 AND remaining <= 5 (filled or overfilled)
//   state='staged'  if loaded == 0 (empty, waiting)
function parseRailcarData(rows) {
  let startingCapacity = null;
  let totalLoaded = null;
  let remainingCapacity = null;
  const cars = [];
  let currentSection = null;

  const CAR_ID_RE = /^[A-Z]{2,4}\s*\d{3,}/;
  const num = v => (typeof v === 'number') ? v : null;

  for (const row of rows) {
    const rowText = row.map(c => String(c || '').toLowerCase()).join(' ');

    // Section detection — section headers live in col 9
    const col9 = String(row[9] || '').trim();
    if (/^load track$/i.test(col9))   currentSection = 'Load Track';
    else if (/^rear track$/i.test(col9)) currentSection = 'Rear Track';
    else if (/derail/i.test(col9))    currentSection = 'Derail Track';

    // Aggregate parsing (label text anywhere in row, value in col 23 = X)
    const xVal = num(row[23]);
    if (xVal !== null) {
      if (rowText.includes('starting') && rowText.includes('track capacity')) {
        startingCapacity = Math.round(xVal * 100) / 100;
      }
      if (rowText.includes('total loaded')) {
        totalLoaded = Math.round(xVal * 100) / 100;
      }
      if (rowText.includes('remaining') && rowText.includes('track capacity')) {
        remainingCapacity = Math.round(xVal * 100) / 100;
      }
    }

    // Per-car row detection
    const carId = String(row[2] || '').trim();
    if (!CAR_ID_RE.test(carId)) continue;
    if (!currentSection) continue;

    const maxBbl    = num(row[4]);
    const safeBbl   = num(row[7]);
    const loadedBbl = num(row[15]) ?? 0;
    const remainBbl = num(row[17]);
    const position  = num(row[1]);

    // Loads (gallons) — filter out nulls/zeros for display
    const loadsGal = [row[8], row[9+0], row[10], row[11], row[12], row[13]]
      .map(v => num(v))
      .filter(v => v !== null && v > 0);

    if (!safeBbl || safeBbl <= 0) continue;

    const remaining = remainBbl !== null ? remainBbl : (safeBbl - loadedBbl);
    let state;
    if (loadedBbl === 0)       state = 'staged';
    else if (remaining <= 5)   state = 'loaded';
    else                       state = 'loading';

    const pctFull = safeBbl > 0
      ? Math.max(0, Math.min(100, Math.round((loadedBbl / safeBbl) * 1000) / 10))
      : 0;

    cars.push({
      section: currentSection,
      position,
      car_id: carId,
      max_cap_bbl: maxBbl != null ? Math.round(maxBbl * 100) / 100 : null,
      safe_cap_bbl: Math.round(safeBbl * 100) / 100,
      loaded_bbl: Math.round(loadedBbl * 100) / 100,
      remaining_bbl: Math.round(remaining * 100) / 100,
      pct_full: pctFull,
      state,
      loads_count: loadsGal.length,
    });
  }

  // Active cars = all "loading" state cars (pumps mid-loading). Typically 0-3.
  const activeCars = cars.filter(c => c.state === 'loading');

  // Totals by state (for future UI / sanity check)
  const counts = {
    total: cars.length,
    staged: cars.filter(c => c.state === 'staged').length,
    loading: activeCars.length,
    loaded: cars.filter(c => c.state === 'loaded').length,
  };

  return {
    startingCapacity,
    totalLoaded,
    remainingCapacity,
    cars,
    active_cars: activeCars,
    counts,
  };
}

// Normalize a railcar ID for matching across tracker and load log
// ("NATX 318028." → "NATX 318028", "UTLX  21342" → "UTLX 21342")
function normalizeCarId(s) {
  return String(s || '').toUpperCase().replace(/\s+/g, ' ').replace(/[.,]+$/g, '').trim();
}

// Read today's load log transactions and return:
//   { byCar: { norm_car_id -> {pump, bol, bbls, pump_start, pump_end} },
//     currentByPump: { "P-101" -> norm_car_id, "P-102" -> ..., "P-103" -> ... } }
// byCar labels any car with its most-recent dispensing pump.
// currentByPump holds ONE car per pump — the one with the LATEST pump_end in today's
// load log, meaning it's the car the pump is currently on (or just finished).
// The ingest/refresh path uses currentByPump to emit exactly ONE active card per pump.
async function getActivePumpByCar(token, env) {
  try {
    const file = await findLoadLog(token, env);
    const { rows, todaySerial } = await readData(token, file.id);

    const byCar = {};
    const latestEndByCar = {};
    const pumpLatest = {};  // pump -> { car, pump_end }

    for (const row of rows) {
      const day = typeof row[1] === 'number' ? Math.floor(row[1]) : null;
      if (day !== todaySerial) continue;
      const carRaw = row[19];
      const carNorm = normalizeCarId(carRaw);
      if (!carNorm) continue;

      const bol = String(row[23] || '').trim();
      let pumpId = null;
      if (bol.startsWith('111')) pumpId = 'P-101';
      else if (bol.startsWith('222')) pumpId = 'P-102';
      else if (bol.startsWith('333')) pumpId = 'P-103';
      if (!pumpId) continue;

      const bbls = parseFloat(row[17]) || 0;
      const pumpStart = typeof row[20] === 'number' ? row[20] : null;
      const pumpEnd = typeof row[21] === 'number' ? row[21] : null;

      // Latest entry per car
      const prevEnd = latestEndByCar[carNorm] ?? -1;
      const thisEnd = pumpEnd ?? -1;
      if (thisEnd >= prevEnd) {
        latestEndByCar[carNorm] = thisEnd;
        byCar[carNorm] = {
          pump: pumpId,
          bol,
          bbls: Math.round(bbls * 100) / 100,
          pump_start: pumpStart,
          pump_end: pumpEnd,
        };
      }

      // Latest car per pump (LATEST pump_end wins)
      const cur = pumpLatest[pumpId];
      if (!cur || thisEnd >= (cur.pump_end ?? -1)) {
        pumpLatest[pumpId] = { car: carNorm, pump_end: pumpEnd };
      }
    }

    const currentByPump = {};
    for (const [pump, v] of Object.entries(pumpLatest)) {
      currentByPump[pump] = v.car;
    }
    return { byCar, currentByPump };
  } catch (e) {
    return { byCar: {}, currentByPump: {}, _error: e.message };
  }
}

async function refreshRailcar(env) {
  const token = await getToken(env);
  const file = await findRailcarTracker(token);
  const capacity = await readRailcarCapacity(token, file.id);

  const pumpResult = await getActivePumpByCar(token, env);
  const byCar = pumpResult.byCar || {};
  const currentByPump = pumpResult.currentByPump || {};
  const pumpLookupError = pumpResult._error || null;

  // Enrich ALL cars with the pump that last touched them (for drilldown)
  const carsEnriched = (capacity.cars || []).map(c => {
    const norm = normalizeCarId(c.car_id);
    const match = byCar[norm];
    return { ...c, pump: match ? match.pump : null };
  });

  // ACTIVE CARS = exactly one per pump, selected by load log's latest pump_end.
  // This replaces the old "all loading state" heuristic that double-counted finished cars.
  const activeCarsEnriched = [];
  for (const [pump, carNorm] of Object.entries(currentByPump)) {
    // Find the car in our enriched list by matching normalized id
    const carInfo = carsEnriched.find(c => normalizeCarId(c.car_id) === carNorm);
    if (!carInfo) continue;
    const match = byCar[carNorm];
    activeCarsEnriched.push({
      ...carInfo,
      pump,
      last_bol: match ? match.bol : null,
      last_load_bbls: match ? match.bbls : null,
    });
  }

  const result = {
    file: file.name,
    file_modified: file.modified,
    refreshed_at: new Date().toISOString(),
    // Daily track aggregates
    starting_capacity: capacity.startingCapacity,
    total_loaded: capacity.totalLoaded,
    remaining_capacity: capacity.remainingCapacity,
    // Per-car breakdown (with pump labels)
    cars: carsEnriched,
    active_cars: activeCarsEnriched,
    counts: capacity.counts || { total: 0, staged: 0, loading: 0, loaded: 0 },
    ...( capacity._error ? { _error: capacity._error } : {}),
    ...( pumpLookupError ? { _pump_lookup_error: pumpLookupError } : {}),
  };

  await env.KV.put('railcar_json', JSON.stringify(result));
  return result;
}

// ── Worker Entry Points ────────────────────────────────────────
// ---- NOAA weather (Cadiz OH) ----
// Two-step API: /points -> forecast URL -> /forecast. Plus /alerts/active.
// Cached 30 min in KV as `weather_json`.
const WEATHER_CACHE_TTL_MS = 30 * 60 * 1000;
const CADIZ_LAT = 40.27;
const CADIZ_LON = -81.00;
const NOAA_UA = 'TimironOps/1.0 (tylerk@timironmp.com)';

async function fetchNoaa(url) {
  const r = await fetch(url, {
    headers: { 'User-Agent': NOAA_UA, 'Accept': 'application/geo+json' },
    cf: { cacheTtl: 60 },
  });
  if (!r.ok) throw new Error(`noaa ${url} HTTP ${r.status}`);
  return r.json();
}

async function getWeather(env, force = false) {
  if (!force) {
    const cached = await env.KV.get('weather_json');
    if (cached) {
      try {
        const obj = JSON.parse(cached);
        const age = Date.now() - new Date(obj.fetched_at).getTime();
        if (age < WEATHER_CACHE_TTL_MS) return { data: obj, from_cache: true };
      } catch {}
    }
  }
  // Cache the gridpoint forecast URL since it's stable per location
  let forecastUrl = await env.KV.get('weather_forecast_url');
  let city = await env.KV.get('weather_city');
  if (!forecastUrl) {
    const points = await fetchNoaa(`https://api.weather.gov/points/${CADIZ_LAT},${CADIZ_LON}`);
    forecastUrl = points?.properties?.forecast;
    city = points?.properties?.relativeLocation?.properties?.city || 'Cadiz';
    if (!forecastUrl) throw new Error('noaa points missing forecast');
    await env.KV.put('weather_forecast_url', forecastUrl);
    await env.KV.put('weather_city', city);
  }

  const [forecast, alerts] = await Promise.all([
    fetchNoaa(forecastUrl),
    fetchNoaa(`https://api.weather.gov/alerts/active?point=${CADIZ_LAT},${CADIZ_LON}`),
  ]);

  const periods = (forecast?.properties?.periods || []).slice(0, 4).map(p => ({
    name: p.name,
    is_daytime: !!p.isDaytime,
    temperature: p.temperature,
    temperature_unit: p.temperatureUnit,
    short_forecast: p.shortForecast,
    detailed_forecast: p.detailedForecast,
    wind_speed: p.windSpeed,
    wind_direction: p.windDirection,
    icon: p.icon || null,
    precip_pct: p.probabilityOfPrecipitation?.value ?? null,
  }));

  const activeAlerts = (alerts?.features || []).map(f => {
    const pp = f.properties || {};
    return {
      event: pp.event,
      severity: pp.severity,
      headline: pp.headline,
      ends: pp.ends || pp.expires,
    };
  });

  const payload = {
    city: city || 'Cadiz',
    state: 'OH',
    periods,
    alerts: activeAlerts,
    source: 'noaa.api.weather.gov',
    fetched_at: new Date().toISOString(),
  };
  await env.KV.put('weather_json', JSON.stringify(payload));
  return { data: payload, from_cache: false };
}

// ---- Crude market data (WTI + Brent) ----
// Uses Yahoo Finance v8 chart endpoint (no auth, returns meta.regularMarketPrice
// + meta.chartPreviousClose). Cached 30 min in KV as `crude_json`.
const CRUDE_CACHE_TTL_MS = 30 * 60 * 1000;
const CRUDE_BROWSER_UA =
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36';

async function fetchYahooChart(symbol) {
  // range=5d interval=1d returns up to 5 daily bars. We take the last two
  // *completed* closes to compute a day-over-day delta. meta.regularMarketPrice
  // is live (may equal today's open bar); meta.chartPreviousClose is the close
  // BEFORE the 5d window starts, so we ignore it and use bar closes directly.
  const url = `https://query1.finance.yahoo.com/v8/finance/chart/${encodeURIComponent(symbol)}?range=5d&interval=1d`;
  const r = await fetch(url, {
    headers: { 'User-Agent': CRUDE_BROWSER_UA, 'Accept': 'application/json' },
    cf: { cacheTtl: 60 },
  });
  if (!r.ok) throw new Error(`yahoo ${symbol} HTTP ${r.status}`);
  const j = await r.json();
  const result = j?.chart?.result?.[0];
  if (!result) throw new Error(`yahoo ${symbol} no result`);
  const meta = result.meta || {};
  const closes = (result.indicators?.quote?.[0]?.close || []).filter(v => typeof v === 'number');
  if (closes.length < 2) throw new Error(`yahoo ${symbol} <2 bars`);
  const latestClose = closes[closes.length - 1];
  const priorClose = closes[closes.length - 2];
  // Prefer live regularMarketPrice if we're mid-session (it beats the in-progress bar close)
  const livePrice = typeof meta.regularMarketPrice === 'number' ? meta.regularMarketPrice : latestClose;
  const delta = livePrice - priorClose;
  const deltaPct = priorClose ? (delta / priorClose) * 100 : 0;
  return {
    symbol,
    price: Math.round(livePrice * 100) / 100,
    prev_close: Math.round(priorClose * 100) / 100,
    delta: Math.round(delta * 100) / 100,
    delta_pct: Math.round(deltaPct * 100) / 100,
    market_time: meta.regularMarketTime ? new Date(meta.regularMarketTime * 1000).toISOString() : null,
  };
}

async function getCrudeQuotes(env, force = false) {
  if (!force) {
    const cached = await env.KV.get('crude_json');
    if (cached) {
      try {
        const obj = JSON.parse(cached);
        const age = Date.now() - new Date(obj.fetched_at).getTime();
        if (age < CRUDE_CACHE_TTL_MS) return { data: obj, from_cache: true };
      } catch {}
    }
  }
  const [wti, brent] = await Promise.all([
    fetchYahooChart('CL=F'),
    fetchYahooChart('BZ=F'),
  ]);
  const payload = {
    wti,
    brent,
    source: 'yahoo-v8-chart',
    fetched_at: new Date().toISOString(),
  };
  await env.KV.put('crude_json', JSON.stringify(payload));
  return { data: payload, from_cache: false };
}

export default {
  // HTTP handler
  async fetch(request, env, ctx) {
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

    // GET /api/atlas — ATLAS narrative + pulse for VERA loader dashboard
    if (url.pathname === '/api/atlas' || url.pathname === '/api/atlas/') {
      const json = await env.KV.get('atlas_json');
      if (!json) return new Response('{"error":"No atlas data yet"}', {
        status: 503, headers: { ...corsHeaders, 'Content-Type': 'application/json' }
      });
      return new Response(json, {
        headers: { ...corsHeaders, 'Content-Type': 'application/json', 'Cache-Control': 'public, max-age=60' }
      });
    }

    // GET /api/safety — daily crew safety briefing (generated on VPS, ingested via POST below)
    if (url.pathname === '/api/safety' || url.pathname === '/api/safety/') {
      const json = await env.KV.get('safety_json');
      if (!json) return new Response('{"error":"No safety data yet"}', {
        status: 503, headers: { ...corsHeaders, 'Content-Type': 'application/json' }
      });
      return new Response(json, {
        headers: { ...corsHeaders, 'Content-Type': 'application/json', 'Cache-Control': 'public, max-age=60' }
      });
    }

    // GET /api/railcars — live railcar capacity + state for VERA Track Sheet
    if (url.pathname === '/api/railcars' || url.pathname === '/api/railcars/') {
      const json = await env.KV.get('railcar_json');
      if (!json) return new Response('{"error":"No railcar data yet — waiting on first ingest or cron refresh"}', {
        status: 503, headers: { ...corsHeaders, 'Content-Type': 'application/json' }
      });
      return new Response(json, {
        headers: { ...corsHeaders, 'Content-Type': 'application/json', 'Cache-Control': 'public, max-age=30' }
      });
    }

    // TEMP DEBUG: GET /api/_debug/rob — can cadiz.ops actually read Rob's files?
    if (url.pathname === '/api/_debug/rob') {
      try {
        const token = await getToken(env);
        async function q(path) {
          const r = await fetch(`${GRAPH_BASE}${path}`, {
            headers: { Authorization: `Bearer ${token}` },
          });
          return { status: r.status, body: await r.json().catch(() => r.text()) };
        }
        const results = {};
        results.rob_root_children = await q('/users/robk@timirontrading.com/drive/root/children?$top=50&$select=name,size,folder,file,webUrl,lastModifiedDateTime');
        // If we got folders, drill into the first one
        if (results.rob_root_children.status === 200) {
          const items = results.rob_root_children.body?.value || [];
          const firstFolder = items.find(i => i.folder);
          if (firstFolder) {
            const folderName = firstFolder.name;
            results.first_folder_name = folderName;
            results.first_folder_contents = await q(`/users/robk@timirontrading.com/drive/root:/${encodeURIComponent(folderName)}:/children?$top=20&$select=name,size,folder,file`);
          }
        }
        return new Response(JSON.stringify(results, null, 2), {
          headers: { ...corsHeaders, 'Content-Type': 'application/json' }
        });
      } catch (e) {
        return new Response(JSON.stringify({ error: e.message, stack: e.stack }), {
          status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' }
        });
      }
    }

    // TEMP DEBUG: GET /api/_debug/whoami — what user identity is behind the Worker token?
    if (url.pathname === '/api/_debug/whoami') {
      try {
        const token = await getToken(env);
        async function q(path) {
          const r = await fetch(`${GRAPH_BASE}${path}`, {
            headers: { Authorization: `Bearer ${token}` },
          });
          return { status: r.status, body: await r.json().catch(() => r.text()) };
        }
        const results = {};
        results.me = await q('/me');
        results.my_drive = await q('/me/drive');
        results.shared_with_me = await q('/me/drive/sharedWithMe?$top=20');
        results.rob_drive = await q('/users/robk@timirontrading.com/drive');
        results.tenant_users = await q('/users?$top=10&$select=displayName,userPrincipalName,mail');
        return new Response(JSON.stringify(results, null, 2), {
          headers: { ...corsHeaders, 'Content-Type': 'application/json' }
        });
      } catch (e) {
        return new Response(JSON.stringify({ error: e.message, stack: e.stack }), {
          status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' }
        });
      }
    }

    // GET /api/tracer — W&LE daily tracer report (KV cache, daily cron refresh)
    if (url.pathname === '/api/tracer' || url.pathname === '/api/tracer/') {
      const json = await env.KV.get('tracer_json');
      if (!json) {
        return new Response(JSON.stringify({ error: 'no tracer data yet — waiting on first daily ingest' }), {
          status: 503, headers: { ...corsHeaders, 'Content-Type': 'application/json' }
        });
      }
      return new Response(json, {
        headers: { ...corsHeaders, 'Content-Type': 'application/json', 'Cache-Control': 'public, max-age=300' }
      });
    }

    // GET /api/tracer/drift — cross-check tracer vs railcar tracker (capacity drift + missing cars)
    if (url.pathname === '/api/tracer/drift') {
      try {
        const tracerJson = await env.KV.get('tracer_json');
        const railcarJson = await env.KV.get('railcar_json');
        if (!tracerJson || !railcarJson) {
          return new Response(JSON.stringify({
            error: 'missing data',
            has_tracer: !!tracerJson,
            has_railcar: !!railcarJson,
          }), { status: 503, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
        }
        const tracer = JSON.parse(tracerJson);
        const railcar = JSON.parse(railcarJson);

        // Build lookup maps (normalized car id -> capacity)
        const tracerByCar = {};
        for (const c of (tracer.our_cars || [])) {
          const norm = normalizeCarId(`${c.car_initial} ${c.car_number}`);
          tracerByCar[norm] = {
            capacity_gal: c.gallonage_capacity,
            capacity_bbl: c.capacity_bbl,
            location: c.current_location,
            waybill: c.bol_number,
          };
        }
        const trackerByCar = {};
        for (const c of (railcar.cars || [])) {
          const norm = normalizeCarId(c.car_id);
          trackerByCar[norm] = {
            max_cap_bbl: c.max_cap_bbl,
            safe_cap_bbl: c.safe_cap_bbl,
            section: c.section,
            position: c.position,
            state: c.state,
          };
        }

        // Drift: cars in BOTH, capacity delta > 5%
        const capacityDrift = [];
        for (const [norm, tr] of Object.entries(trackerByCar)) {
          const tc = tracerByCar[norm];
          if (!tc) continue;
          const trackerBbl = tr.max_cap_bbl;
          const tracerBbl = tc.capacity_bbl;
          if (!trackerBbl || !tracerBbl) continue;
          const delta = Math.abs(trackerBbl - tracerBbl);
          const pct = (delta / tracerBbl) * 100;
          if (pct > 5) {
            capacityDrift.push({
              car_id: norm,
              tracker_bbl: trackerBbl,
              tracer_bbl: Math.round(tracerBbl * 100) / 100,
              delta_bbl: Math.round(delta * 100) / 100,
              pct: Math.round(pct * 10) / 10,
              section: tr.section,
              position: tr.position,
            });
          }
        }

        // Missing in tracer: tracker car not in tracer (loader typo or stale tracker)
        const missingInTracer = [];
        for (const [norm, tr] of Object.entries(trackerByCar)) {
          if (!tracerByCar[norm] && tr.state !== 'staged') {
            missingInTracer.push({
              car_id: norm,
              section: tr.section,
              position: tr.position,
              state: tr.state,
              max_cap_bbl: tr.max_cap_bbl,
            });
          }
        }

        // Missing in tracker: tracer car at NELMS not in tracker (should be on-site but loader hasn't logged)
        const missingInTracker = [];
        for (const [norm, tc] of Object.entries(tracerByCar)) {
          if (tc.location === 'NELMS' && !trackerByCar[norm]) {
            missingInTracker.push({
              car_id: norm,
              tracer_capacity_bbl: tc.capacity_bbl,
              waybill: tc.waybill,
            });
          }
        }

        return new Response(JSON.stringify({
          generated_at: new Date().toISOString(),
          tracer_generated_at: tracer.generated_at,
          tracker_refreshed_at: railcar.refreshed_at,
          tracker_source: railcar.source,
          summary: {
            tracker_cars: Object.keys(trackerByCar).length,
            tracer_cars: Object.keys(tracerByCar).length,
            capacity_drift_count: capacityDrift.length,
            missing_in_tracer_count: missingInTracer.length,
            missing_in_tracker_count: missingInTracker.length,
          },
          capacity_drift: capacityDrift,
          missing_in_tracer: missingInTracer,
          missing_in_tracker: missingInTracker,
        }, null, 2), {
          headers: { ...corsHeaders, 'Content-Type': 'application/json' }
        });
      } catch (e) {
        return new Response(JSON.stringify({ error: e.message, stack: e.stack }), {
          status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' }
        });
      }
    }

    // POST /api/tracer/ingest — VPS posts the daily W&LE tracer report
    // Body: { rows: [[...],...], generated_at: "...", source: "vps-nyx-tracer" }
    if (url.pathname === '/api/tracer/ingest' && request.method === 'POST') {
      const auth = request.headers.get('X-Tracer-Token');
      // Prefer Worker secret (no KV write cost). Falls back to KV for backwards compat.
      const expected = env.TRACER_INGEST_TOKEN || await env.KV.get('tracer_ingest_token');
      if (!expected || auth !== expected) {
        return new Response('{"error":"unauthorized"}', {
          status: 401, headers: { ...corsHeaders, 'Content-Type': 'application/json' }
        });
      }
      try {
        const body = await request.json();
        const rows = Array.isArray(body.rows) ? body.rows : null;
        if (!rows || rows.length === 0) {
          return new Response(JSON.stringify({ error: 'rows array required' }), {
            status: 400, headers: { ...corsHeaders, 'Content-Type': 'application/json' }
          });
        }

        // Row 0 is header, rest are data. Parse into structured objects.
        // Column layout per reference_wle_tracer_report.md
        const dataRows = rows.slice(1);
        const allCars = [];
        for (const r of dataRows) {
          if (!r || r.length < 21) continue;
          const carInitial = String(r[0] || '').trim();
          const carNumberRaw = r[1];
          const carNumber = (typeof carNumberRaw === 'number')
            ? String(Math.round(carNumberRaw))
            : String(carNumberRaw || '').trim().replace(/\.0+$/, '');
          if (!carInitial || !carNumber) continue;
          const gallons = typeof r[20] === 'number' ? r[20] : parseFloat(r[20]) || 0;
          allCars.push({
            car_initial: carInitial,
            car_number: carNumber,
            car_id: `${carInitial} ${carNumber}`,
            last_move_type: String(r[2] || '').trim(),
            last_move_date: String(r[3] || '').trim(),
            last_move_time: r[4] || null,
            train_track: String(r[5] || '').trim(),
            current_location: String(r[6] || '').trim(),
            le: String(r[7] || '').trim(),
            consignee: String(r[10] || '').trim(),
            destination: String(r[11] || '').trim(),
            gross_weight_lb: typeof r[18] === 'number' ? r[18] : null,
            gallonage_capacity: gallons,
            capacity_bbl: gallons > 0 ? Math.round((gallons / 42) * 100) / 100 : null,
            equip_type: String(r[21] || '').trim(),
            bill_to_patron: String(r[25] || '').trim(),
            bol_number: String(r[28] || '').trim(),
          });
        }

        // Filter to our cars (TIMIRON LLC, destination NELMS)
        const ourCars = allCars.filter(c =>
          c.consignee.toUpperCase() === 'TIMIRON LLC' &&
          c.destination.toUpperCase() === 'NELMS'
        );

        // Bucket by current_location for pipeline view
        const byLocation = {};
        for (const c of ourCars) {
          const loc = c.current_location || 'UNKNOWN';
          if (!byLocation[loc]) byLocation[loc] = [];
          byLocation[loc].push(c.car_id);
        }
        const pipeline = {};
        for (const [loc, ids] of Object.entries(byLocation)) {
          pipeline[loc] = ids.length;
        }

        const result = {
          generated_at: new Date().toISOString(),
          source: body.source || 'vps-nyx-tracer',
          email_date: body.email_date || null,
          file: body.file || null,
          total_cars: allCars.length,
          our_cars_count: ourCars.length,
          pipeline,
          all_cars: allCars,
          our_cars: ourCars,
          by_location: byLocation,
        };

        // Delta-write guard: skip KV put if tracer data matches last write.
        // The tracer usually only changes once per day (next morning's email), so this
        // protects against accidental double-runs hitting the write quota.
        let wroteKv = false;
        try {
          const prevJson = await env.KV.get('tracer_json');
          const prev = prevJson ? JSON.parse(prevJson) : null;
          const prevCarKey = prev ? JSON.stringify((prev.our_cars || []).map(c => c.car_id + ':' + c.current_location).sort()) : '';
          const newCarKey = JSON.stringify(ourCars.map(c => c.car_id + ':' + c.current_location).sort());
          if (!prev || prevCarKey !== newCarKey || prev.total_cars !== result.total_cars) {
            await env.KV.put('tracer_json', JSON.stringify(result));
            wroteKv = true;
          }
        } catch (e) {
          return new Response(JSON.stringify({
            ok: true,
            kv_write_skipped: true,
            kv_error: String(e.message || e),
            total_rows: rows.length,
            our_cars: ourCars.length,
            note: 'kv write blocked (quota or error) — data not persisted this cycle',
          }), { headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
        }

        return new Response(JSON.stringify({
          ok: true,
          wrote_kv: wroteKv,
          total_rows: rows.length,
          total_cars: allCars.length,
          our_cars: ourCars.length,
          pipeline,
          generated_at: result.generated_at,
        }), {
          headers: { ...corsHeaders, 'Content-Type': 'application/json' }
        });
      } catch (e) {
        return new Response(JSON.stringify({ error: e.message, stack: e.stack }), {
          status: 400, headers: { ...corsHeaders, 'Content-Type': 'application/json' }
        });
      }
    }

    // POST /api/railcar/ingest — TIMMY (Cadiz PC) posts live Excel rows
    // Body: { rows: [[cell, cell, ...], ...], source: "cadiz-timmy", file: "...", ts: "..." }
    // Token header: X-Railcar-Token (value stored in KV as 'railcar_ingest_token')
    if (url.pathname === '/api/railcar/ingest' && request.method === 'POST') {
      const auth = request.headers.get('X-Railcar-Token');
      const expected = await env.KV.get('railcar_ingest_token');
      if (!expected || auth !== expected) {
        return new Response('{"error":"unauthorized"}', {
          status: 401, headers: { ...corsHeaders, 'Content-Type': 'application/json' }
        });
      }
      try {
        const body = await request.json();
        const rows = Array.isArray(body.rows) ? body.rows : null;
        if (!rows || rows.length === 0) {
          return new Response(JSON.stringify({ error: 'rows array required' }), {
            status: 400, headers: { ...corsHeaders, 'Content-Type': 'application/json' }
          });
        }

        // Parse the raw rows server-side using the same parser the Graph API path uses
        const capacity = parseRailcarData(rows);

        // Pull pump map from KV cache if fresh (<60s old) to keep ingest fast.
        // TIMMY POSTs every 30s; cache lets us amortize the Graph API cost.
        let pumpResult = { byCar: {}, currentByPump: {} };
        let pumpLookupError = null;
        try {
          const cached = await env.KV.get('pump_by_car_cache', { type: 'json' });
          const now = Date.now();
          if (cached && cached.ts && (now - cached.ts) < 60_000 && cached.data) {
            pumpResult = cached.data;
          } else {
            const token = await getToken(env);
            pumpResult = await getActivePumpByCar(token, env);
            await env.KV.put('pump_by_car_cache', JSON.stringify({
              ts: now,
              data: pumpResult,
            }));
          }
          pumpLookupError = pumpResult._error || null;
        } catch (e) {
          pumpLookupError = e.message;
        }

        const byCar = pumpResult.byCar || {};
        const currentByPump = pumpResult.currentByPump || {};

        // Enrich all cars with pump label
        const carsEnriched = (capacity.cars || []).map(c => {
          const norm = normalizeCarId(c.car_id);
          const match = byCar[norm];
          return { ...c, pump: match ? match.pump : null };
        });

        // ACTIVE CARS = exactly one per pump, by load log's latest pump_end
        const activeCarsEnriched = [];
        for (const [pump, carNorm] of Object.entries(currentByPump)) {
          const carInfo = carsEnriched.find(c => normalizeCarId(c.car_id) === carNorm);
          if (!carInfo) continue;
          const match = byCar[carNorm];
          activeCarsEnriched.push({
            ...carInfo,
            pump,
            last_bol: match ? match.bol : null,
            last_load_bbls: match ? match.bbls : null,
          });
        }

        const result = {
          file: body.file || 'Cadiz Ops live Excel',
          file_modified: body.ts || new Date().toISOString(),
          refreshed_at: new Date().toISOString(),
          source: body.source || 'cadiz-timmy',
          starting_capacity: capacity.startingCapacity,
          total_loaded: capacity.totalLoaded,
          remaining_capacity: capacity.remainingCapacity,
          cars: carsEnriched,
          active_cars: activeCarsEnriched,
          counts: capacity.counts || { total: 0, staged: 0, loading: 0, loaded: 0 },
          ...( pumpLookupError ? { _pump_lookup_error: pumpLookupError } : {}),
        };

        // Delta-write guard: skip KV put if data is identical to last write.
        // Cloudflare Workers free tier: 1000 KV writes/day. TIMMY posts every 2 min =
        // 720 potential writes/day, so delta-gating drops to ~10-50/day during real ops.
        let wroteKv = false;
        try {
          const prevJson = await env.KV.get('railcar_json');
          const prev = prevJson ? JSON.parse(prevJson) : null;
          const changed = !prev
            || prev.starting_capacity !== result.starting_capacity
            || prev.total_loaded !== result.total_loaded
            || prev.remaining_capacity !== result.remaining_capacity
            || JSON.stringify(prev.counts || {}) !== JSON.stringify(result.counts || {})
            || JSON.stringify((prev.active_cars || []).map(c => c.car_id + ':' + c.loaded_bbl).sort())
               !== JSON.stringify((result.active_cars || []).map(c => c.car_id + ':' + c.loaded_bbl).sort());
          if (changed) {
            await env.KV.put('railcar_json', JSON.stringify(result));
            wroteKv = true;
          }
        } catch (e) {
          // If the KV write itself fails (quota exceeded), return success with a flag
          // so TIMMY keeps polling but doesn't log errors every cycle.
          return new Response(JSON.stringify({
            ok: true,
            kv_write_skipped: true,
            kv_error: String(e.message || e),
            counts: result.counts,
            note: 'kv write blocked (quota or error) — data not persisted this cycle',
          }), { headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
        }

        return new Response(JSON.stringify({
          ok: true,
          wrote_kv: wroteKv,
          counts: result.counts,
          starting_capacity: result.starting_capacity,
          total_loaded: result.total_loaded,
          remaining_capacity: result.remaining_capacity,
          active_pumps: activeCarsEnriched.map(c => c.pump).filter(Boolean),
          refreshed_at: result.refreshed_at,
        }), {
          headers: { ...corsHeaders, 'Content-Type': 'application/json' }
        });
      } catch (e) {
        return new Response(JSON.stringify({ error: e.message, stack: e.stack }), {
          status: 400, headers: { ...corsHeaders, 'Content-Type': 'application/json' }
        });
      }
    }

    // POST /api/safety/ingest — VPS posts the daily safety briefing
    if (url.pathname === '/api/safety/ingest' && request.method === 'POST') {
      const auth = request.headers.get('X-Safety-Token');
      const expected = await env.KV.get('safety_ingest_token');
      if (!expected || auth !== expected) {
        return new Response('{"error":"unauthorized"}', {
          status: 401, headers: { ...corsHeaders, 'Content-Type': 'application/json' }
        });
      }
      try {
        const body = await request.json();
        const payload = {
          headline: body.headline || '',
          reminders: Array.isArray(body.reminders) ? body.reminders : [],
          inputs: body.inputs || null,
          updated_at: new Date().toISOString(),
        };
        await env.KV.put('safety_json', JSON.stringify(payload));
        return new Response(JSON.stringify({ ok: true, stored: Object.keys(payload) }), {
          headers: { ...corsHeaders, 'Content-Type': 'application/json' }
        });
      } catch (e) {
        return new Response(JSON.stringify({ error: e.message }), {
          status: 400, headers: { ...corsHeaders, 'Content-Type': 'application/json' }
        });
      }
    }

    // GET /api/weather — Cadiz OH NOAA forecast + alerts (30-min KV cache)
    if (url.pathname === '/api/weather' || url.pathname === '/api/weather/') {
      const force = url.searchParams.get('force') === '1';
      try {
        const { data, from_cache } = await getWeather(env, force);
        return new Response(JSON.stringify({ ...data, from_cache }), {
          headers: {
            ...corsHeaders,
            'Content-Type': 'application/json',
            'Cache-Control': 'public, max-age=300',
          },
        });
      } catch (e) {
        const stale = await env.KV.get('weather_json');
        if (stale) {
          const obj = JSON.parse(stale);
          return new Response(JSON.stringify({ ...obj, from_cache: true, stale: true, fetch_error: e.message }), {
            status: 200,
            headers: { ...corsHeaders, 'Content-Type': 'application/json' },
          });
        }
        return new Response(JSON.stringify({ error: e.message }), {
          status: 502,
          headers: { ...corsHeaders, 'Content-Type': 'application/json' },
        });
      }
    }

    // GET /api/crude — live WTI + Brent from Yahoo (30-min KV cache)
    if (url.pathname === '/api/crude' || url.pathname === '/api/crude/') {
      const force = url.searchParams.get('force') === '1';
      try {
        const { data, from_cache } = await getCrudeQuotes(env, force);
        return new Response(JSON.stringify({ ...data, from_cache }), {
          headers: {
            ...corsHeaders,
            'Content-Type': 'application/json',
            'Cache-Control': 'public, max-age=300',
          },
        });
      } catch (e) {
        const stale = await env.KV.get('crude_json');
        if (stale) {
          const obj = JSON.parse(stale);
          return new Response(JSON.stringify({ ...obj, from_cache: true, stale: true, fetch_error: e.message }), {
            status: 200,
            headers: { ...corsHeaders, 'Content-Type': 'application/json' },
          });
        }
        return new Response(JSON.stringify({ error: e.message }), {
          status: 502,
          headers: { ...corsHeaders, 'Content-Type': 'application/json' },
        });
      }
    }

    // POST /api/atlas/ingest — briefing script posts narrative + pulse + hero KPIs
    if (url.pathname === '/api/atlas/ingest' && request.method === 'POST') {
      const auth = request.headers.get('X-Atlas-Token');
      const expected = await env.KV.get('atlas_ingest_token');
      if (!expected || auth !== expected) {
        return new Response('{"error":"unauthorized"}', {
          status: 401, headers: { ...corsHeaders, 'Content-Type': 'application/json' }
        });
      }
      try {
        const body = await request.json();
        const payload = {
          narrative: body.narrative || '',
          pulse: body.pulse || null,
          hero_kpis: body.hero_kpis || null,
          market_context: body.market_context || '',
          updated_at: new Date().toISOString(),
        };
        await env.KV.put('atlas_json', JSON.stringify(payload));
        return new Response(JSON.stringify({ ok: true, stored: Object.keys(payload) }), {
          headers: { ...corsHeaders, 'Content-Type': 'application/json' }
        });
      } catch (e) {
        return new Response(JSON.stringify({ error: e.message }), {
          status: 400, headers: { ...corsHeaders, 'Content-Type': 'application/json' }
        });
      }
    }

    // POST /api/refresh — trigger immediate refresh (bypasses 60-min crew throttle)
    if (url.pathname === '/api/debug') {
      try {
        const token = await getToken(env);
        const file = await findLoadLog(token, env);
        const { rows, startSerial, todaySerial, now: eastNow } = await readData(token, file.id);
        const daySerials = [...new Set(rows.map(r => Math.floor(r[1])))].sort();
        const days = daySerials.map(s => {
          const d = serialToDate(s);
          const count = rows.filter(r => Math.floor(r[1]) === s).length;
          return { serial: s, date: fmtDate(d), rows: count };
        });
        // Check multiple ranges to find today's data
        const rawUrl = `${GRAPH_BASE}/me/drive/items/${file.id}/workbook/worksheets('Master_Load_Log')/range(address='B2:B500')`;
        const rawR = await fetch(rawUrl, { headers: { Authorization: `Bearer ${token}` } });
        let rawLast = [];
        if (rawR.ok) {
          const rawData = await rawR.json();
          // Find rows with today's serial OR non-number dates (potential issues)
          const allVals = (rawData.values || []).map((r, i) => ({ row: i+2, val: r[0], type: typeof r[0] }));
          const withData = allVals.filter(r => r.val);
          rawLast = withData.slice(-15);
        }
        return new Response(JSON.stringify({
          file: file.name, modified: file.modified,
          calculatedToday: todaySerial, cstDate: fmtDate(cstNow),
          totalRows: rows.length,
          days, colB_last15: rawLast
        }, null, 2), { headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
      } catch (e) {
        return new Response(JSON.stringify({ error: e.message }), {
          status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' }
        });
      }
    }

    // Alert endpoint — watcher reads this and forwards to Telegram
    if (url.pathname === '/api/alerts') {
      const alert = await env.KV.get('load_log_alert');
      const staleCount = await env.KV.get('dashboard_stale_count') || '0';
      const lastRefresh = await env.KV.get('last_refresh');
      const pinnedName = await env.KV.get('load_log_file_name');
      return new Response(JSON.stringify({
        alert: alert ? JSON.parse(alert) : null,
        stale_count: parseInt(staleCount),
        last_refresh: lastRefresh,
        pinned_file: pinnedName,
      }), { headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
    }

    // Clear alert after watcher has read it
    if (url.pathname === '/api/alerts/ack' && request.method === 'POST') {
      await env.KV.delete('load_log_alert');
      return new Response(JSON.stringify({ ok: true }), {
        headers: { ...corsHeaders, 'Content-Type': 'application/json' }
      });
    }

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

    // GET /api/daily-crew — daily punch-level data for Corporate App
    if (url.pathname === '/api/daily-crew' || url.pathname === '/api/daily-crew/') {
      // Try cache first
      const nowMs2 = Date.now();
      const nowUTC2 = new Date(nowMs2);
      const yr2 = nowUTC2.getUTCFullYear();
      const m1 = new Date(Date.UTC(yr2, 2, 1));
      const ds2 = new Date(Date.UTC(yr2, 2, 14 - m1.getUTCDay(), 7));
      const n1 = new Date(Date.UTC(yr2, 10, 1));
      const de2 = new Date(Date.UTC(yr2, 10, 7 - n1.getUTCDay(), 6));
      const isDST2 = nowMs2 >= ds2.getTime() && nowMs2 < de2.getTime();
      const etOff2 = (isDST2 ? -4 : -5) * 3600000;
      const todayKey = new Date(nowMs2 + etOff2).toISOString().split('T')[0];

      const skipCache = url.searchParams.has('fresh');
      const cached = skipCache ? null : await env.KV.get(`daily-crew-${todayKey}`);
      if (cached) {
        return new Response(cached, {
          headers: { ...corsHeaders, 'Content-Type': 'application/json', 'Cache-Control': 'public, max-age=60' }
        });
      }
      // Live fetch
      try {
        const result = await refreshDailyCrew(env);
        if (!result) return new Response('{"error":"No QBT token configured"}', {
          status: 503, headers: { ...corsHeaders, 'Content-Type': 'application/json' }
        });
        return new Response(JSON.stringify(result), {
          headers: { ...corsHeaders, 'Content-Type': 'application/json', 'Cache-Control': 'public, max-age=60' }
        });
      } catch (e) {
        return new Response(JSON.stringify({ error: e.message }), {
          status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' }
        });
      }
    }

    // GET /api/railcar — railcar capacity tracker
    // ?force=1 bypasses KV and does a live fetch (for debugging parser)
    if (url.pathname === '/api/railcar' || url.pathname === '/api/railcar/') {
      const forceLive = url.searchParams.get('force') === '1';
      const json = forceLive ? null : await env.KV.get('railcar_json');
      if (!json) {
        // Try live fetch
        try {
          const result = await refreshRailcar(env);
          return new Response(JSON.stringify(result), {
            headers: { ...corsHeaders, 'Content-Type': 'application/json' }
          });
        } catch (e) {
          return new Response(JSON.stringify({ error: e.message }), {
            status: 503, headers: { ...corsHeaders, 'Content-Type': 'application/json' }
          });
        }
      }
      return new Response(json, {
        headers: { ...corsHeaders, 'Content-Type': 'application/json', 'Cache-Control': 'public, max-age=60' }
      });
    }

    // GET /api/files — browse cadiz_ops OneDrive
    if (url.pathname === '/api/files' || url.pathname.startsWith('/api/files/')) {
      try {
        const token = await getToken(env);
        const path = url.pathname.replace('/api/files', '').replace(/^\//, '') || '';
        const query = url.searchParams.get('q');
        const readFile = url.searchParams.get('read'); // file ID to read content
        const readRange = url.searchParams.get('range'); // Excel range to read
        const readSheet = url.searchParams.get('sheet'); // Excel sheet name

        // Read specific Excel range
        if (readFile && readRange) {
          const sheet = readSheet ? `'${encodeURIComponent(readSheet)}'` : '';
          const rangeUrl = sheet
            ? `${GRAPH_BASE}/me/drive/items/${readFile}/workbook/worksheets(${sheet})/range(address='${readRange}')`
            : `${GRAPH_BASE}/me/drive/items/${readFile}/workbook/worksheets/range(address='${readRange}')`;
          const r = await fetch(rangeUrl, { headers: { Authorization: `Bearer ${token}` } });
          if (!r.ok) return new Response(JSON.stringify({ error: `Range read failed: ${r.status}` }), {
            status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' }
          });
          const data = await r.json();
          return new Response(JSON.stringify({ file_id: readFile, range: readRange, sheet: readSheet, values: data.values }), {
            headers: { ...corsHeaders, 'Content-Type': 'application/json' }
          });
        }

        // Get download URL for a file (for PDF/Word reading)
        const download = url.searchParams.get('download');
        if (download) {
          const dlUrl = `${GRAPH_BASE}/me/drive/items/${download}/content`;
          const r = await fetch(dlUrl, { headers: { Authorization: `Bearer ${token}` }, redirect: 'manual' });
          // Graph API returns a 302 redirect to the actual download URL
          const location = r.headers.get('Location') || r.headers.get('location');
          if (location) {
            return new Response(JSON.stringify({ file_id: download, download_url: location }), {
              headers: { ...corsHeaders, 'Content-Type': 'application/json' }
            });
          }
          // If no redirect, try to stream the content
          const content = await fetch(dlUrl, { headers: { Authorization: `Bearer ${token}` } });
          if (content.ok) {
            return new Response(JSON.stringify({ file_id: download, download_url: content.url }), {
              headers: { ...corsHeaders, 'Content-Type': 'application/json' }
            });
          }
          return new Response(JSON.stringify({ error: 'Could not get download URL' }), {
            status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' }
          });
        }

        // List worksheets in an Excel file
        if (readFile && !readRange) {
          const wsUrl = `${GRAPH_BASE}/me/drive/items/${readFile}/workbook/worksheets`;
          const r = await fetch(wsUrl, { headers: { Authorization: `Bearer ${token}` } });
          if (r.ok) {
            const data = await r.json();
            const sheets = (data.value || []).map(s => s.name);
            return new Response(JSON.stringify({ file_id: readFile, sheets }), {
              headers: { ...corsHeaders, 'Content-Type': 'application/json' }
            });
          }
          return new Response(JSON.stringify({ error: 'Not an Excel file or cannot read sheets' }), {
            status: 400, headers: { ...corsHeaders, 'Content-Type': 'application/json' }
          });
        }

        // Search files
        if (query) {
          const r = await fetch(`${GRAPH_BASE}/me/drive/root/search(q='${encodeURIComponent(query)}')`, {
            headers: { Authorization: `Bearer ${token}` }
          });
          if (!r.ok) throw new Error('Search failed');
          const data = await r.json();
          const files = (data.value || []).map(f => ({
            id: f.id, name: f.name, size: f.size,
            modified: f.lastModifiedDateTime,
            folder: !!f.folder,
            path: f.parentReference?.path?.replace('/drive/root:', '') || '',
          }));
          return new Response(JSON.stringify({ query, results: files }), {
            headers: { ...corsHeaders, 'Content-Type': 'application/json' }
          });
        }

        // List folder contents
        const listUrl = path
          ? `${GRAPH_BASE}/me/drive/root:/${path}:/children?$top=100&$select=id,name,size,lastModifiedDateTime,folder,file`
          : `${GRAPH_BASE}/me/drive/root/children?$top=100&$select=id,name,size,lastModifiedDateTime,folder,file`;
        const r = await fetch(listUrl, { headers: { Authorization: `Bearer ${token}` } });
        if (!r.ok) throw new Error(`List failed: ${r.status}`);
        const data = await r.json();
        const items = (data.value || []).map(f => ({
          id: f.id, name: f.name, size: f.size,
          modified: f.lastModifiedDateTime,
          folder: !!f.folder,
          type: f.file?.mimeType || (f.folder ? 'folder' : 'unknown'),
        }));
        return new Response(JSON.stringify({ path: path || '/', items }), {
          headers: { ...corsHeaders, 'Content-Type': 'application/json' }
        });
      } catch (e) {
        return new Response(JSON.stringify({ error: e.message }), {
          status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' }
        });
      }
    }

    // GET /api/status — last refresh time
    if (url.pathname === '/api/status') {
      const lastRefresh = await env.KV.get('last_refresh');
      return new Response(JSON.stringify({ last_refresh: lastRefresh }), {
        headers: { ...corsHeaders, 'Content-Type': 'application/json' }
      });
    }

    // ========================================================================
    // FLAGMAN admin endpoints (token-gated via training-portal admin role)
    // ------------------------------------------------------------------------
    // /api/flagman/admin/training - proxy to training-portal dashboard-public
    // /api/flagman/admin/crew     - aggregate KV inspection + chat per-crew
    // /api/flagman/admin/photos   - extract r2_keys from inspections + chat
    // Auth: ?token=<crew_token> must resolve to crew.role === 'admin'
    // ========================================================================
    if (url.pathname.startsWith('/api/flagman/admin/')) {
      // Admin gate
      const adminToken = (url.searchParams.get('token') || '').trim();
      if (!adminToken) {
        return new Response(JSON.stringify({ error: 'missing_token' }), {
          status: 403, headers: { ...corsHeaders, 'Content-Type': 'application/json' }
        });
      }
      let adminOk = false;
      let adminCrewName = null;
      try {
        const r = await fetch(`https://training.kolassus.ai/api/crew/${encodeURIComponent(adminToken)}`, {
          headers: { 'User-Agent': 'FLAGMAN/1.0' },
        });
        if (r.status === 200) {
          const j = await r.json();
          if (j && j.crew && j.crew.role === 'admin') {
            adminOk = true;
            adminCrewName = j.crew.name || null;
          }
        }
      } catch (e) { /* fall through */ }
      if (!adminOk) {
        return new Response(JSON.stringify({ error: 'forbidden' }), {
          status: 403, headers: { ...corsHeaders, 'Content-Type': 'application/json' }
        });
      }

      // Route 1: training dashboard proxy (KV-cached 60s)
      if (url.pathname === '/api/flagman/admin/training') {
        const cacheKey = 'flagman:admin:training_cache';
        const cached = await env.KV.get(cacheKey);
        if (cached) {
          return new Response(cached, {
            headers: { ...corsHeaders, 'Content-Type': 'application/json', 'X-Cache': 'HIT' }
          });
        }
        const bearer = env.FLAGMAN_BEARER;
        if (!bearer) {
          return new Response(JSON.stringify({ error: 'bearer_not_configured' }), {
            status: 503, headers: { ...corsHeaders, 'Content-Type': 'application/json' }
          });
        }
        try {
          const upstream = await fetch('https://training.kolassus.ai/api/admin/dashboard-public', {
            headers: {
              'Authorization': `Bearer ${bearer}`,
              'User-Agent': 'FLAGMAN/1.0',
            },
          });
          const body = await upstream.text();
          if (upstream.status === 200) {
            await env.KV.put(cacheKey, body, { expirationTtl: 60 });
          }
          return new Response(body, {
            status: upstream.status,
            headers: { ...corsHeaders, 'Content-Type': 'application/json', 'X-Cache': 'MISS' }
          });
        } catch (e) {
          return new Response(JSON.stringify({ error: 'upstream_failed', detail: e.message }), {
            status: 502, headers: { ...corsHeaders, 'Content-Type': 'application/json' }
          });
        }
      }

      // Route 2: per-crew aggregate from KV
      if (url.pathname === '/api/flagman/admin/crew') {
        const insRaw = await env.KV.get('flagman:inspections:recent');
        const chatRaw = await env.KV.get('flagman:chat:recent');
        let inspections = [];
        let chats = [];
        try { inspections = insRaw ? JSON.parse(insRaw) : []; } catch (e) { inspections = []; }
        try { chats = chatRaw ? JSON.parse(chatRaw) : []; } catch (e) { chats = []; }
        if (!Array.isArray(inspections)) inspections = [];
        if (!Array.isArray(chats)) chats = [];

        const byToken = new Map();
        const ensure = (tok) => {
          if (!byToken.has(tok)) {
            byToken.set(tok, {
              token: tok, name: null,
              total_inspections: 0, total_chats: 0,
              last_inspection_id: null, last_inspection_ts: null,
              last_chat_ts: null, last_seen: null,
            });
          }
          return byToken.get(tok);
        };
        for (const i of inspections) {
          const tok = (i.crew_token || '').toString();
          if (!tok) continue;
          const e = ensure(tok);
          e.total_inspections++;
          if (!e.last_inspection_ts || (i.timestamp && i.timestamp > e.last_inspection_ts)) {
            e.last_inspection_ts = i.timestamp || null;
            e.last_inspection_id = i.inspection_id || null;
          }
        }
        for (const c of chats) {
          const tok = (c.crew_token || '').toString();
          if (!tok) continue;
          const e = ensure(tok);
          e.total_chats++;
          if (c.crew_name && !e.name) e.name = c.crew_name;
          if (!e.last_chat_ts || (c.timestamp && c.timestamp > e.last_chat_ts)) {
            e.last_chat_ts = c.timestamp || null;
          }
        }

        // Resolve names from KV cache or training-portal lookup (best effort)
        const out = [];
        for (const [tok, agg] of byToken) {
          if (!agg.name) {
            agg.name = await env.KV.get(`flagman:crew_name:${tok}`);
          }
          if (!agg.name) {
            try {
              const r = await fetch(`https://training.kolassus.ai/api/crew/${encodeURIComponent(tok)}`, {
                headers: { 'User-Agent': 'FLAGMAN/1.0' },
              });
              if (r.status === 200) {
                const j = await r.json();
                if (j && j.crew && j.crew.name) {
                  agg.name = String(j.crew.name).replace(/[^\x20-\x7E]/g, '');
                  await env.KV.put(`flagman:crew_name:${tok}`, agg.name, { expirationTtl: 86400 });
                }
              }
            } catch (e) { /* ignore */ }
          }
          if (!agg.name) {
            agg.name = tok.replace(/-/g, ' ').replace(/\b\w/g, c => c.toUpperCase());
          }
          // last_seen = max(last_inspection_ts, last_chat_ts)
          const a = agg.last_inspection_ts || '';
          const b = agg.last_chat_ts || '';
          agg.last_seen = a > b ? a : (b || null);
          out.push(agg);
        }
        out.sort((a, b) => (b.last_seen || '').localeCompare(a.last_seen || ''));
        return new Response(JSON.stringify({ count: out.length, crew: out }), {
          headers: { ...corsHeaders, 'Content-Type': 'application/json' }
        });
      }

      // Route 2b: leaderboard - per-crew aggregate w/ 7d/30d/all + streak + defects
      if (url.pathname === '/api/flagman/admin/leaderboard') {
        const insRaw = await env.KV.get('flagman:inspections:recent');
        const chatRaw = await env.KV.get('flagman:chat:recent');
        let inspections = [];
        let chats = [];
        try { inspections = insRaw ? JSON.parse(insRaw) : []; } catch (e) { inspections = []; }
        try { chats = chatRaw ? JSON.parse(chatRaw) : []; } catch (e) { chats = []; }
        if (!Array.isArray(inspections)) inspections = [];
        if (!Array.isArray(chats)) chats = [];

        const now = Date.now();
        const day = 24 * 3600 * 1000;
        const t7 = now - 7 * day;
        const t30 = now - 30 * day;

        const byTok = new Map();
        const ensure = (tok, name) => {
          if (!byTok.has(tok)) {
            byTok.set(tok, {
              token: tok, name: name || tok,
              ins7: 0, ins30: 0, ins_all: 0,
              defects: 0, chats: 0,
              days: new Set(),
              last_inspection_ts: null,
            });
          }
          const r = byTok.get(tok);
          if (name && r.name === tok) r.name = name;
          return r;
        };

        for (const i of inspections) {
          const tok = (i.crew_token || '').toString();
          if (!tok) continue;
          const r = ensure(tok, i.crew_name);
          r.ins_all++;
          const ts = i.timestamp ? Date.parse(i.timestamp) : null;
          if (ts && ts >= t7) r.ins7++;
          if (ts && ts >= t30) r.ins30++;
          if (ts && (!r.last_inspection_ts || ts > r.last_inspection_ts)) r.last_inspection_ts = ts;
          if (ts) r.days.add(new Date(ts).toISOString().slice(0, 10));
          const photos = Array.isArray(i.photos) ? i.photos : [];
          if (photos.length || (i.notes && String(i.notes).trim().length > 5)) r.defects++;
        }
        for (const c of chats) {
          const tok = (c.crew_token || '').toString();
          if (!tok) continue;
          const r = ensure(tok, c.crew_name);
          r.chats++;
        }

        const out = [];
        for (const r of byTok.values()) {
          // Streak: consecutive days ending today/yesterday with at least 1 inspection
          const days = Array.from(r.days).sort().reverse();
          let streak = 0;
          const todayMid = new Date(); todayMid.setUTCHours(0,0,0,0);
          let cursor = todayMid.getTime();
          const todayIso = new Date(cursor).toISOString().slice(0, 10);
          const yIso = new Date(cursor - day).toISOString().slice(0, 10);
          if (days[0] === todayIso || days[0] === yIso) {
            if (days[0] === yIso) cursor -= day;
            for (let i = 0; i < days.length; i++) {
              const expect = new Date(cursor - i * day).toISOString().slice(0, 10);
              if (days[i] === expect) streak++; else break;
            }
          }
          delete r.days;
          r.streak = streak;
          out.push(r);
        }
        out.sort((a, b) => b.ins_all - a.ins_all);
        return new Response(JSON.stringify({ count: out.length, leaderboard: out }), {
          headers: { ...corsHeaders, 'Content-Type': 'application/json' }
        });
      }

      // Route 3: photo manifest (r2 keys from both inspections and chat)
      if (url.pathname === '/api/flagman/admin/photos') {
        const insRaw = await env.KV.get('flagman:inspections:recent');
        const chatRaw = await env.KV.get('flagman:chat:recent');
        let inspections = [];
        let chats = [];
        try { inspections = insRaw ? JSON.parse(insRaw) : []; } catch (e) { inspections = []; }
        try { chats = chatRaw ? JSON.parse(chatRaw) : []; } catch (e) { chats = []; }
        if (!Array.isArray(inspections)) inspections = [];
        if (!Array.isArray(chats)) chats = [];

        const photos = [];
        for (const i of inspections) {
          const ph = Array.isArray(i.photos) ? i.photos : [];
          for (const p of ph) {
            const key = (typeof p === 'string') ? p : (p && p.r2_key) || (p && p.key) || null;
            if (!key) continue;
            photos.push({
              key: key,
              source: 'inspection',
              timestamp: i.timestamp || null,
              crew_token: i.crew_token || null,
              ref_id: i.inspection_id || null,
            });
          }
        }
        for (const c of chats) {
          const keys = Array.isArray(c.photo_keys) ? c.photo_keys : [];
          for (const k of keys) {
            if (!k || typeof k !== 'string') continue;
            photos.push({
              key: k,
              source: 'chat',
              timestamp: c.timestamp || null,
              crew_token: c.crew_token || null,
              ref_id: c.id || null,
            });
          }
        }
        photos.sort((a, b) => (b.timestamp || '').localeCompare(a.timestamp || ''));
        const trimmed = photos.slice(0, 100);
        return new Response(JSON.stringify({ count: trimmed.length, photos: trimmed }), {
          headers: { ...corsHeaders, 'Content-Type': 'application/json' }
        });
      }

      return new Response(JSON.stringify({ error: 'unknown_admin_route' }), {
        status: 404, headers: { ...corsHeaders, 'Content-Type': 'application/json' }
      });
    }

    // ───────────────────────────────────────────────────────────────────────
    // OAuth re-consent admin routes (2026-05-05 — Files.ReadWrite.All + Mail.Send)
    // bearer-gated via FLAGMAN_BEARER. /start kicks off auth-code flow,
    // /callback exchanges code and writes refresh_token to KV, /probe smoke-tests.
    // ───────────────────────────────────────────────────────────────────────
    if (url.pathname === '/api/admin/oauth/start') {
      const bearer = url.searchParams.get('bearer') || '';
      if (!env.FLAGMAN_BEARER || bearer !== env.FLAGMAN_BEARER) {
        return new Response('forbidden', { status: 403, headers: corsHeaders });
      }
      const redirectUri = `${url.origin}/api/admin/oauth/callback`;
      const nonce = crypto.randomUUID();
      await env.KV.put('oauth_nonce', nonce, { expirationTtl: 600 });
      const authUrl = new URL('https://login.microsoftonline.com/132a8676-8518-49e8-885a-ea8d5ec0a533/oauth2/v2.0/authorize');
      authUrl.searchParams.set('client_id', env.CLIENT_ID);
      authUrl.searchParams.set('response_type', 'code');
      authUrl.searchParams.set('redirect_uri', redirectUri);
      authUrl.searchParams.set('response_mode', 'query');
      authUrl.searchParams.set('scope', SCOPE);
      authUrl.searchParams.set('state', nonce);
      authUrl.searchParams.set('prompt', 'consent');
      return Response.redirect(authUrl.toString(), 302);
    }
    if (url.pathname === '/api/admin/oauth/callback') {
      const code = url.searchParams.get('code');
      const state = url.searchParams.get('state');
      const err = url.searchParams.get('error');
      if (err) {
        return new Response(`OAuth error: ${err} - ${url.searchParams.get('error_description') || ''}`, { status: 400, headers: corsHeaders });
      }
      if (!code) return new Response('missing code', { status: 400, headers: corsHeaders });
      const expectedNonce = await env.KV.get('oauth_nonce');
      if (!expectedNonce || state !== expectedNonce) {
        return new Response('state_mismatch', { status: 400, headers: corsHeaders });
      }
      const redirectUri = `${url.origin}/api/admin/oauth/callback`;
      const tokenResp = await fetch(TOKEN_URL, {
        method: 'POST',
        headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
        body: new URLSearchParams({
          client_id: env.CLIENT_ID,
          client_secret: env.ATLAS_CLIENT_SECRET,
          grant_type: 'authorization_code',
          code: code,
          redirect_uri: redirectUri,
          scope: SCOPE,
        }),
      });
      const tokenData = await tokenResp.json();
      if (!tokenData.refresh_token) {
        return new Response(JSON.stringify({ error: 'no_refresh_token', detail: tokenData }, null, 2), { status: 500, headers: { 'Content-Type': 'application/json', ...corsHeaders } });
      }
      await env.KV.put('refresh_token', tokenData.refresh_token);
      await env.KV.put('oauth_consented_at', new Date().toISOString());
      await env.KV.put('oauth_consented_scope', tokenData.scope || 'unknown');
      await env.KV.delete('oauth_nonce');
      return new Response(`<html><body style="font-family:system-ui;padding:2em;"><h2>OAuth re-consent OK</h2><p>refresh_token written to KV.</p><p>Scopes granted: <code>${(tokenData.scope || '').replace(/</g,'&lt;')}</code></p><p>You can close this tab.</p></body></html>`, { headers: { 'Content-Type': 'text/html', ...corsHeaders } });
    }
    if (url.pathname === '/api/admin/oauth/probe') {
      const bearer = url.searchParams.get('bearer') || '';
      if (!env.FLAGMAN_BEARER || bearer !== env.FLAGMAN_BEARER) {
        return new Response('forbidden', { status: 403, headers: corsHeaders });
      }
      try {
        const tk = await getToken(env);
        const consentedAt = await env.KV.get('oauth_consented_at');
        const consentedScope = await env.KV.get('oauth_consented_scope');
        const tempPath = `/me/drive/root:/_oauth_probe_${Date.now()}.txt:/content`;
        const putR = await fetch(`${GRAPH_BASE}${tempPath}`, {
          method: 'PUT',
          headers: { Authorization: `Bearer ${tk}`, 'Content-Type': 'text/plain' },
          body: 'oauth probe',
        });
        let writeOk = putR.ok, writeStatus = putR.status, deletedOk = false;
        if (putR.ok) {
          const item = await putR.json();
          const delR = await fetch(`${GRAPH_BASE}/me/drive/items/${item.id}`, {
            method: 'DELETE',
            headers: { Authorization: `Bearer ${tk}` },
          });
          deletedOk = delR.ok;
        }
        return new Response(JSON.stringify({
          ok: true,
          consented_at: consentedAt,
          consented_scope: consentedScope,
          files_readwrite_probe: { ok: writeOk, status: writeStatus },
          delete_cleanup: deletedOk,
        }, null, 2), { headers: { 'Content-Type': 'application/json', ...corsHeaders } });
      } catch (e) {
        return new Response(JSON.stringify({ ok: false, error: e.message }, null, 2), { status: 500, headers: { 'Content-Type': 'application/json', ...corsHeaders } });
      }
    }

    // GET /api/flagman/_debug/replay/<id>?bearer=<FLAGMAN_BEARER> — Task 6 manual replay.
    // Re-runs PDF + OneDrive + email export for a single inspection. Idempotency marker
    // is cleared first so this always re-exports.
    if (url.pathname.startsWith('/api/flagman/_debug/replay/')) {
      const bearer = url.searchParams.get('bearer') || '';
      if (!env.FLAGMAN_BEARER || bearer !== env.FLAGMAN_BEARER) {
        return new Response('forbidden', { status: 403, headers: corsHeaders });
      }
      const id = url.pathname.split('/').pop();
      if (!id) return new Response(JSON.stringify({ error: 'missing id' }), { status: 400, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
      const raw = await env.KV.get(`flagman:inspection:${id}`);
      if (!raw) return new Response(JSON.stringify({ error: 'not_found' }), { status: 404, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
      const record = JSON.parse(raw);
      await env.KV.delete(`flagman:exported:${id}`);
      const result = await exportInspectionToOneDriveAndEmail(record, env);
      return new Response(JSON.stringify(result, null, 2), {
        status: result.ok ? 200 : 500,
        headers: { ...corsHeaders, 'Content-Type': 'application/json' },
      });
    }

    // POST /api/flagman/submit — crew inspection submit pipeline
    // Writes to KV (persistence), relays to VERA (realtime kiosk), pings NYX on emergent.
    // Task 6 (2026-05-05): on success, enqueue PDF + OneDrive + email export via ctx.waitUntil.
    // See: docs/specs/2026-04-17-flagman-submit-pipeline.md
    if (url.pathname === '/api/flagman/submit' && request.method === 'POST') {
      try {
        const payload = await request.json();
        const result = await handleFlagmanSubmit(payload, env);
        if (result.status === 'ok' && result.inspection_id && ctx && ctx.waitUntil) {
          ctx.waitUntil((async () => {
            try {
              const raw = await env.KV.get(`flagman:inspection:${result.inspection_id}`);
              if (raw) {
                const record = JSON.parse(raw);
                await exportInspectionToOneDriveAndEmail(record, env);
              }
            } catch (e) {
              await env.KV.put(`flagman:export_error:${result.inspection_id}`, JSON.stringify({ error: e.message, at: new Date().toISOString() }), { expirationTtl: 7 * 86400 });
            }
          })());
        }
        return new Response(JSON.stringify(result), {
          status: result.status === 'ok' ? 200 : 400,
          headers: { ...corsHeaders, 'Content-Type': 'application/json' }
        });
      } catch (e) {
        return new Response(JSON.stringify({ status: 'error', error: e.message }), {
          status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' }
        });
      }
    }

    // GET /api/flagman/pending-ocr?token=<adminToken>&limit=<N>
    // Admin-gated. Returns inspections that have frame_keys but no ocr block yet.
    if (url.pathname === '/api/flagman/pending-ocr' || url.pathname === '/api/flagman/pending-ocr/') {
      const token = (url.searchParams.get('token') || '').trim();
      const limitParam = url.searchParams.get('limit');
      const limit = limitParam !== null ? Number(limitParam) : null;
      const result = await handleFlagmanPendingOcr({ token, limit }, env);
      return new Response(JSON.stringify(result.body), {
        status: result.status,
        headers: { ...corsHeaders, 'Content-Type': 'application/json' },
      });
    }

    // POST /api/flagman/inspection/<id>/ocr?token=<adminToken>
    // Admin-gated. Attaches/overwrites ocr block on a stored inspection record.
    if (request.method === 'POST' && /^\/api\/flagman\/inspection\/[^/]+\/ocr\/?$/.test(url.pathname)) {
      const parts = url.pathname.replace(/\/$/, '').split('/');
      // path: ['', 'api', 'flagman', 'inspection', <id>, 'ocr']
      const id = parts[4] || '';
      const token = (url.searchParams.get('token') || '').trim();
      let ocr = {};
      try {
        const body = await request.json();
        ocr = (body && typeof body.ocr === 'object' && body.ocr !== null) ? body.ocr : {};
      } catch (_) { /* treat missing/invalid body as empty ocr */ }
      const result = await handleFlagmanOcrWriteback({ token, id, ocr }, env);
      return new Response(JSON.stringify(result.body), {
        status: result.status,
        headers: { ...corsHeaders, 'Content-Type': 'application/json' },
      });
    }

    // GET /api/flagman/recent — list last 100 inspections (for VERA backfill / PWA history)
    if (url.pathname === '/api/flagman/recent' || url.pathname === '/api/flagman/recent/') {
      const recentRaw = await env.KV.get('flagman:inspections:recent');
      const recent = recentRaw ? JSON.parse(recentRaw) : [];
      return new Response(JSON.stringify({ count: recent.length, inspections: recent }), {
        headers: { ...corsHeaders, 'Content-Type': 'application/json' }
      });
    }

    // GET /api/flagman/inspection/:id — single inspection by ID
    if (url.pathname.startsWith('/api/flagman/inspection/')) {
      const id = url.pathname.split('/').pop();
      if (!id) {
        return new Response(JSON.stringify({ error: 'Missing inspection_id' }), {
          status: 400, headers: { ...corsHeaders, 'Content-Type': 'application/json' }
        });
      }
      const raw = await env.KV.get(`flagman:inspection:${id}`);
      if (!raw) {
        return new Response(JSON.stringify({ error: 'Not found' }), {
          status: 404, headers: { ...corsHeaders, 'Content-Type': 'application/json' }
        });
      }
      return new Response(raw, {
        headers: { ...corsHeaders, 'Content-Type': 'application/json' }
      });
    }

    // POST /api/flagman/photo - multipart photo upload to R2
    // form fields: crew_token (str), inspection_id (str, optional), file (image/*)
    // returns { status: 'ok', r2_key }
    if (url.pathname === '/api/flagman/photo' && request.method === 'POST') {
      try {
        if (!env.FLAGMAN_PHOTOS) {
          return new Response(JSON.stringify({ status: 'error', error: 'R2 binding FLAGMAN_PHOTOS not configured' }), {
            status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' }
          });
        }
        const form = await request.formData();
        const crewToken = (form.get('crew_token') || '').toString().trim();
        const inspectionId = (form.get('inspection_id') || 'pending').toString().trim();
        const file = form.get('file');
        if (!crewToken) {
          return new Response(JSON.stringify({ status: 'error', error: 'Missing crew_token' }), {
            status: 400, headers: { ...corsHeaders, 'Content-Type': 'application/json' }
          });
        }
        if (!file || typeof file === 'string') {
          return new Response(JSON.stringify({ status: 'error', error: 'Missing file' }), {
            status: 400, headers: { ...corsHeaders, 'Content-Type': 'application/json' }
          });
        }
        const ctype = file.type || 'application/octet-stream';
        if (!ctype.startsWith('image/')) {
          return new Response(JSON.stringify({ status: 'error', error: 'Only image/* content-type accepted' }), {
            status: 400, headers: { ...corsHeaders, 'Content-Type': 'application/json' }
          });
        }
        if (file.size > 10 * 1024 * 1024) {
          return new Response(JSON.stringify({ status: 'error', error: 'File exceeds 10MB' }), {
            status: 413, headers: { ...corsHeaders, 'Content-Type': 'application/json' }
          });
        }
        const safeToken = crewToken.replace(/[^a-zA-Z0-9_-]/g, '_');
        const safeInsp = inspectionId.replace(/[^a-zA-Z0-9_:.-]/g, '_');
        const safeName = (file.name || 'photo.jpg').replace(/[^a-zA-Z0-9_.-]/g, '_');
        const stamp = Date.now();
        const r2Key = `${safeToken}/${safeInsp}/${stamp}_${safeName}`;
        await env.FLAGMAN_PHOTOS.put(r2Key, file.stream(), {
          httpMetadata: { contentType: ctype }
        });
        return new Response(JSON.stringify({ status: 'ok', r2_key: r2Key }), {
          headers: { ...corsHeaders, 'Content-Type': 'application/json' }
        });
      } catch (e) {
        return new Response(JSON.stringify({ status: 'error', error: e.message }), {
          status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' }
        });
      }
    }

    // POST /api/flagman/chat - crew chat post (text + optional photo r2 keys)
    if (url.pathname === '/api/flagman/chat' && request.method === 'POST') {
      try {
        const payload = await request.json();
        const result = await handleFlagmanChat(payload, env);
        return new Response(JSON.stringify(result), {
          status: result.status === 'ok' ? 200 : 400,
          headers: { ...corsHeaders, 'Content-Type': 'application/json' }
        });
      } catch (e) {
        return new Response(JSON.stringify({ status: 'error', error: e.message }), {
          status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' }
        });
      }
    }

    // GET /api/flagman/chat/recent?since=<ms-or-ISO>
    if (url.pathname === '/api/flagman/chat/recent' || url.pathname === '/api/flagman/chat/recent/') {
      const recentRaw = await env.KV.get('flagman:chat:recent');
      let messages = recentRaw ? JSON.parse(recentRaw) : [];
      if (!Array.isArray(messages)) messages = [];
      const sinceParam = url.searchParams.get('since');
      if (sinceParam) {
        let sinceMs = 0;
        const n = Number(sinceParam);
        if (!isNaN(n) && n > 0) sinceMs = n;
        else { const d = Date.parse(sinceParam); if (!isNaN(d)) sinceMs = d; }
        if (sinceMs > 0) {
          messages = messages.filter(m => {
            const t = Date.parse(m.timestamp || '');
            return !isNaN(t) && t > sinceMs;
          });
        }
      } else {
        messages = messages.slice(0, 50);
      }
      return new Response(JSON.stringify({ count: messages.length, messages: messages }), {
        headers: { ...corsHeaders, 'Content-Type': 'application/json' }
      });
    }

    // GET /api/flagman/chat/photo/<r2_key> - proxy R2 fetch as image
    if (url.pathname.startsWith('/api/flagman/chat/photo/')) {
      try {
        if (!env.FLAGMAN_PHOTOS) {
          return new Response('R2 binding not configured', { status: 500, headers: corsHeaders });
        }
        const key = decodeURIComponent(url.pathname.replace('/api/flagman/chat/photo/', ''));
        if (!key) return new Response('Missing key', { status: 400, headers: corsHeaders });
        const obj = await env.FLAGMAN_PHOTOS.get(key);
        if (!obj) return new Response('Not found', { status: 404, headers: corsHeaders });
        const ctype = (obj.httpMetadata && obj.httpMetadata.contentType) || 'image/jpeg';
        return new Response(obj.body, {
          headers: {
            ...corsHeaders,
            'Content-Type': ctype,
            'Cache-Control': 'public, max-age=3600'
          }
        });
      } catch (e) {
        return new Response('Photo proxy error: ' + e.message, { status: 500, headers: corsHeaders });
      }
    }

    // POST /api/flagman/time_off - crew time-off request -> OneDrive JSON drop
    if (url.pathname === '/api/flagman/time_off' && request.method === 'POST') {
      try {
        const payload = await request.json();
        const result = await handleFlagmanTimeOff(payload, env);
        return new Response(JSON.stringify(result), {
          status: result.status === 'ok' ? 200 : (result.status === 'unauthorized' ? 401 : 400),
          headers: { ...corsHeaders, 'Content-Type': 'application/json' },
        });
      } catch (e) {
        return new Response(JSON.stringify({ status: 'error', error: 'Bad JSON: ' + e.message }), {
          status: 400, headers: { ...corsHeaders, 'Content-Type': 'application/json' },
        });
      }
    }

    // GET /api/flagman/hours?token=<crew_token> - per-crew hours, current pay period
    if (url.pathname === '/api/flagman/hours' || url.pathname === '/api/flagman/hours/') {
      try {
        const crewToken = (url.searchParams.get('token') || '').trim();
        const result = await handleFlagmanHours(crewToken, env);
        return new Response(JSON.stringify(result), {
          status: result.status === 'ok' ? 200 : (result.status === 'unauthorized' ? 401 : 400),
          headers: { ...corsHeaders, 'Content-Type': 'application/json' },
        });
      } catch (e) {
        return new Response(JSON.stringify({ status: 'error', error: 'Hours fetch failed: ' + e.message }), {
          status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' },
        });
      }
    }

    return new Response('Not found', { status: 404, headers: corsHeaders });
  },

  // Cron handler - runs every 30 min
  async scheduled(event, env, ctx) {
    ctx.waitUntil(Promise.all([
      refreshDashboard(env),
      refreshRailcar(env).catch(e => console.error('Railcar refresh:', e.message)),
      // Force-refresh market + weather caches so PWA loads stay fresh even without visitors
      getCrudeQuotes(env, true).catch(e => console.error('Crude refresh:', e.message)),
      getWeather(env, true).catch(e => console.error('Weather refresh:', e.message)),
    ]));
  },
};

// ============================================================================
// FLAGMAN inspection submit handler
// ----------------------------------------------------------------------------
// Validates payload, generates inspection_id, persists to KV, prepends summary
// to the recent-100 list. Photos are uploaded separately via /api/flagman/photo
// and their R2 keys are referenced in payload.photos.
// ============================================================================
async function handleFlagmanSubmit(payload, env) {
  if (!payload || typeof payload !== 'object') {
    return { status: 'error', error: 'Invalid payload' };
  }
  const crewToken = (payload.crew_token || '').toString().trim();
  if (!crewToken) {
    return { status: 'error', error: 'Missing crew_token' };
  }
  // Back-compat: inspection_type falls back to checklist_id, then "daily"
  const inspectionType = (payload.inspection_type || payload.checklist_id || 'daily').toString().trim();

  // Task 2: validate crew_token against training-portal crew API.
  // Cached in KV (flagman:token_valid:<token>) with 1hr TTL.
  const cacheKey = `flagman:token_valid:${crewToken}`;
  let tokenValid = await env.KV.get(cacheKey);
  let crewName = null;
  if (tokenValid === null) {
    try {
      const resp = await fetch(`https://training.kolassus.ai/api/crew/${encodeURIComponent(crewToken)}`, {
        headers: { 'User-Agent': 'FLAGMAN/1.0' },
      });
      if (resp.status === 200) {
        tokenValid = '1';
        await env.KV.put(cacheKey, '1', { expirationTtl: 3600 });
        try {
          const body = await resp.json();
          crewName = body.name || body.crew_name || null;
        } catch (_) {}
      } else if (resp.status === 404) {
        tokenValid = '0';
        await env.KV.put(cacheKey, '0', { expirationTtl: 3600 });
      } else {
        // Upstream error — fail closed
        return { status: 'error', error: 'Token validation upstream error' };
      }
    } catch (e) {
      return { status: 'error', error: 'Token validation failed' };
    }
  }
  if (tokenValid !== '1') {
    return { status: 'error', error: 'Invalid crew token' };
  }

  // Idempotency: if submission_id present and already stored, return cached result
  let submissionId = payload.submission_id ? payload.submission_id.toString().trim() : null;
  if (submissionId && submissionId.length > 128) submissionId = submissionId.slice(0, 128);
  if (submissionId) {
    const dedupKey = `flagman:submitted:${submissionId}`;
    const existing = await env.KV.get(dedupKey);
    if (existing !== null) {
      try {
        const stored = JSON.parse(existing);
        return { status: 'ok', inspection_id: stored.inspection_id };
      } catch (_) {}
    }
  }

  const ts = (payload.timestamp || new Date().toISOString()).toString();
  const rand = Math.random().toString(36).slice(2, 6);
  const safeToken = crewToken.replace(/[^a-zA-Z0-9_-]/g, '_');
  const inspectionId = `${ts.replace(/[:.]/g, '-')}_${safeToken}_${rand}`;

  // Build flat photos array (back-compat union): legacy top-level + items photo_keys + frame_keys
  const legacyPhotos = Array.isArray(payload.photos) ? payload.photos : [];
  const itemsPhotos = Array.isArray(payload.items)
    ? payload.items.flatMap(item => Array.isArray(item.photo_keys) ? item.photo_keys : [])
    : [];
  const frameKeys = Array.isArray(payload.frame_keys) ? payload.frame_keys : [];
  const photosUnion = [...new Set([...legacyPhotos, ...itemsPhotos, ...frameKeys])];

  // Resolve crew_name — try KV cache if not resolved from token lookup
  if (!crewName) {
    crewName = await env.KV.get(`flagman:crew_name:${crewToken}`);
  }

  const record = {
    inspection_id: inspectionId,
    crew_token: crewToken,
    crew_name: crewName || null,
    inspection_type: inspectionType,
    notes: (payload.notes || '').toString(),
    photos: photosUnion,
    timestamp: ts,
    location: payload.location || null,
    gps: payload.gps || null,
    received_at: new Date().toISOString(),
    // Rich fields (new, optional)
    submission_id: submissionId || null,
    asset: payload.asset || null,
    checklist_id: payload.checklist_id || null,
    direction: payload.direction || null,
    railcar_number: payload.railcar_number || null,
    items: Array.isArray(payload.items) ? payload.items : [],
    frame_keys: frameKeys,
    source: payload.source || null,
  };

  // Persist full record
  await env.KV.put(`flagman:inspection:${inspectionId}`, JSON.stringify(record));

  // Store dedup key for idempotency (30-day TTL)
  if (submissionId) {
    await env.KV.put(`flagman:submitted:${submissionId}`, JSON.stringify({ inspection_id: inspectionId }), { expirationTtl: 30 * 86400 });
  }

  // Prepend summary to recent-100 list (extended fields for PWA Recent tab + VERA)
  const summary = {
    inspection_id: inspectionId,
    crew_token: crewToken,
    crew_name: crewName || null,
    inspection_type: inspectionType,
    timestamp: ts,
    photo_count: photosUnion.length,
    location: record.location,
    asset: record.asset,
    railcar_number: record.railcar_number,
  };
  let recent = [];
  try {
    const recentRaw = await env.KV.get('flagman:inspections:recent');
    if (recentRaw) recent = JSON.parse(recentRaw);
    if (!Array.isArray(recent)) recent = [];
  } catch (e) {
    recent = [];
  }
  recent.unshift(summary);
  if (recent.length > 100) recent = recent.slice(0, 100);
  await env.KV.put('flagman:inspections:recent', JSON.stringify(recent));

  return { status: 'ok', inspection_id: inspectionId };
}

// ============================================================================
// FLAGMAN chat post handler (Task 4)
// ----------------------------------------------------------------------------
// Group-only chat: validates token, ASCII-strips text, persists message + appends
// summary to flagman:chat:recent (cap 200, newest-first).
// ============================================================================
async function handleFlagmanChat(payload, env) {
  if (!payload || typeof payload !== 'object') {
    return { status: 'error', error: 'Invalid payload' };
  }
  const crewToken = (payload.crew_token || '').toString().trim();
  if (!crewToken) return { status: 'error', error: 'Missing crew_token' };

  let text = (payload.text || '').toString();
  // ASCII-only enforcement: strip any byte > 0x7E or below 0x09 (keep tab, LF, CR)
  text = text.replace(/[^\x09\x0A\x0D\x20-\x7E]/g, '').trim();
  const photoKeys = Array.isArray(payload.photo_keys)
    ? payload.photo_keys.filter(k => typeof k === 'string' && k.length).slice(0, 8)
    : [];
  if (!text && photoKeys.length === 0) {
    return { status: 'error', error: 'Empty message' };
  }
  if (text.length > 2000) text = text.slice(0, 2000);

  // Validate token via training-portal (cached)
  const cacheKey = `flagman:token_valid:${crewToken}`;
  let tokenValid = await env.KV.get(cacheKey);
  let crewName = null;
  const nameCacheKey = `flagman:crew_name:${crewToken}`;
  if (tokenValid === null) {
    try {
      const resp = await fetch(`https://training.kolassus.ai/api/crew/${encodeURIComponent(crewToken)}`, {
        headers: { 'User-Agent': 'FLAGMAN/1.0' },
      });
      if (resp.status === 200) {
        tokenValid = '1';
        await env.KV.put(cacheKey, '1', { expirationTtl: 3600 });
        try {
          const j = await resp.json();
          if (j && j.name) {
            crewName = String(j.name).replace(/[^\x20-\x7E]/g, '');
            await env.KV.put(nameCacheKey, crewName, { expirationTtl: 86400 });
          }
        } catch (e) { /* ignore */ }
      } else if (resp.status === 404) {
        tokenValid = '0';
        await env.KV.put(cacheKey, '0', { expirationTtl: 3600 });
      } else {
        return { status: 'error', error: 'Token validation upstream error' };
      }
    } catch (e) {
      return { status: 'error', error: 'Token validation failed' };
    }
  }
  if (tokenValid !== '1') return { status: 'error', error: 'Invalid crew token' };
  if (!crewName) crewName = await env.KV.get(nameCacheKey);
  if (!crewName) {
    // derive from slug
    crewName = crewToken.replace(/-/g, ' ').replace(/\b\w/g, c => c.toUpperCase());
  }

  const ts = new Date().toISOString();
  const tsMs = Date.now();
  const safeToken = crewToken.replace(/[^a-zA-Z0-9_-]/g, '_');
  const rand = Math.random().toString(36).slice(2, 6);
  const messageId = `chat_${tsMs}_${safeToken}_${rand}`;

  const message = {
    id: messageId,
    crew_name: crewName,
    crew_token: crewToken,
    text: text,
    photo_keys: photoKeys,
    timestamp: ts,
  };

  await env.KV.put(`flagman:chat:${messageId}`, JSON.stringify(message));

  let recent = [];
  try {
    const recentRaw = await env.KV.get('flagman:chat:recent');
    if (recentRaw) recent = JSON.parse(recentRaw);
    if (!Array.isArray(recent)) recent = [];
  } catch (e) { recent = []; }
  recent.unshift(message);
  if (recent.length > 200) recent = recent.slice(0, 200);
  await env.KV.put('flagman:chat:recent', JSON.stringify(recent));

  return { status: 'ok', message_id: messageId };
}

// ============================================================================
// FLAGMAN time-off submit handler
// ----------------------------------------------------------------------------
// Validates crew_token, resolves crew_name, writes a JSON request file to
// OneDrive at Timiron/_muster/time_off/<slug>-<YYYYMMDD-HHMM>.json. Mirrors to
// KV (muster:time_off:<id> + recent cap 200) for durability if Graph PUT fails.
// ============================================================================
async function handleFlagmanTimeOff(payload, env) {
  if (!payload || typeof payload !== 'object') {
    return { status: 'error', error: 'Invalid payload' };
  }
  const crewToken = (payload.crew_token || '').toString().trim();
  if (!crewToken) return { status: 'unauthorized', error: 'Missing crew_token' };

  const startDate = (payload.start_date || '').toString().trim();
  const endDate = (payload.end_date || '').toString().trim();
  let reason = (payload.reason || '').toString();

  const dateRe = /^\d{4}-\d{2}-\d{2}$/;
  if (!dateRe.test(startDate) || !dateRe.test(endDate)) {
    return { status: 'error', error: 'start_date and end_date must be YYYY-MM-DD' };
  }
  if (endDate < startDate) {
    return { status: 'error', error: 'end_date before start_date' };
  }
  reason = reason.replace(/[^\x09\x0A\x0D\x20-\x7E]/g, '').trim();
  if (reason.length > 1000) reason = reason.slice(0, 1000);

  const cacheKey = `flagman:token_valid:${crewToken}`;
  let tokenValid = await env.KV.get(cacheKey);
  let crewName = null;
  const nameCacheKey = `flagman:crew_name:${crewToken}`;
  if (tokenValid === null) {
    try {
      const resp = await fetch(`https://training.kolassus.ai/api/crew/${encodeURIComponent(crewToken)}`, {
        headers: { 'User-Agent': 'FLAGMAN/1.0' },
      });
      if (resp.status === 200) {
        tokenValid = '1';
        await env.KV.put(cacheKey, '1', { expirationTtl: 3600 });
        try {
          const j = await resp.json();
          if (j && j.name) {
            crewName = String(j.name).replace(/[^\x20-\x7E]/g, '');
            await env.KV.put(nameCacheKey, crewName, { expirationTtl: 86400 });
          }
        } catch (e) { /* ignore */ }
      } else if (resp.status === 404) {
        tokenValid = '0';
        await env.KV.put(cacheKey, '0', { expirationTtl: 3600 });
      } else {
        return { status: 'error', error: 'Token validation upstream error' };
      }
    } catch (e) {
      return { status: 'error', error: 'Token validation failed' };
    }
  }
  if (tokenValid !== '1') return { status: 'unauthorized', error: 'Invalid crew token' };
  if (!crewName) crewName = await env.KV.get(nameCacheKey);
  if (!crewName) {
    crewName = crewToken.replace(/-/g, ' ').replace(/\b\w/g, c => c.toUpperCase());
  }

  const now = new Date();
  const tsIso = now.toISOString();
  const safeToken = crewToken.replace(/[^a-zA-Z0-9_-]/g, '_');
  const stamp = tsIso.replace(/[-:T]/g, '').slice(0, 13).replace(/(\d{8})(\d{4})/, '$1-$2');
  const requestId = `to_${stamp}_${safeToken}`;

  const record = {
    request_id: requestId,
    crew_token: crewToken,
    crew_name: crewName,
    crew_slug: crewToken,
    start_date: startDate,
    end_date: endDate,
    reason: reason,
    submitted_at: tsIso,
  };

  await env.KV.put(`muster:time_off:${requestId}`, JSON.stringify(record));
  let toRecent = [];
  try {
    const raw = await env.KV.get('muster:time_off:recent');
    if (raw) toRecent = JSON.parse(raw);
    if (!Array.isArray(toRecent)) toRecent = [];
  } catch (e) { toRecent = []; }
  toRecent.unshift({
    request_id: requestId,
    crew_name: crewName,
    crew_token: crewToken,
    start_date: startDate,
    end_date: endDate,
    submitted_at: tsIso,
  });
  if (toRecent.length > 200) toRecent = toRecent.slice(0, 200);
  await env.KV.put('muster:time_off:recent', JSON.stringify(toRecent));

  let oneDriveStatus = 'queued';
  let oneDriveError = null;
  try {
    const writeToken = await getWriteToken(env);
    const fileName = `${safeToken}-${stamp}.json`;
    const onedrivePath = `Timiron/_muster/time_off/${fileName}`;
    // Encode segments individually so '/' separators stay literal
    const encPath = onedrivePath.split('/').map(encodeURIComponent).join('/');
    const putUrl = `${GRAPH_BASE}/me/drive/root:/${encPath}:/content`;
    const putResp = await fetch(putUrl, {
      method: 'PUT',
      headers: {
        Authorization: `Bearer ${writeToken}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(record, null, 2),
    });
    if (putResp.ok) {
      oneDriveStatus = 'ok';
    } else {
      oneDriveStatus = 'failed';
      oneDriveError = `Graph PUT ${putResp.status}`;
    }
  } catch (e) {
    oneDriveStatus = 'failed';
    oneDriveError = e.message || 'unknown';
  }

  return {
    status: 'ok',
    request_id: requestId,
    onedrive: oneDriveStatus,
    onedrive_error: oneDriveError,
  };
}

// ============================================================================
// FLAGMAN hours read handler
// ----------------------------------------------------------------------------
// Returns the requesting crew's hours for the current Mon-Sun pay period.
// Pulls qbt xlsx from OneDrive via Graph workbook usedRange; filters strictly
// to crew_name resolved from token. Never returns other crews' rows.
// ============================================================================
async function handleFlagmanHours(crewToken, env) {
  if (!crewToken) return { status: 'unauthorized', error: 'Missing crew_token' };

  const cacheKey = `flagman:token_valid:${crewToken}`;
  let tokenValid = await env.KV.get(cacheKey);
  let crewName = null;
  const nameCacheKey = `flagman:crew_name:${crewToken}`;
  if (tokenValid === null) {
    try {
      const resp = await fetch(`https://training.kolassus.ai/api/crew/${encodeURIComponent(crewToken)}`, {
        headers: { 'User-Agent': 'FLAGMAN/1.0' },
      });
      if (resp.status === 200) {
        tokenValid = '1';
        await env.KV.put(cacheKey, '1', { expirationTtl: 3600 });
        try {
          const j = await resp.json();
          if (j && j.name) {
            crewName = String(j.name).replace(/[^\x20-\x7E]/g, '');
            await env.KV.put(nameCacheKey, crewName, { expirationTtl: 86400 });
          }
        } catch (e) { /* ignore */ }
      } else if (resp.status === 404) {
        tokenValid = '0';
        await env.KV.put(cacheKey, '0', { expirationTtl: 3600 });
      } else {
        return { status: 'error', error: 'Token validation upstream error' };
      }
    } catch (e) {
      return { status: 'error', error: 'Token validation failed' };
    }
  }
  if (tokenValid !== '1') return { status: 'unauthorized', error: 'Invalid crew token' };
  if (!crewName) crewName = await env.KV.get(nameCacheKey);
  // Hours requires verified crew_name from training-portal. Slug-derived names
  // are unreliable for QBT row matching; fail closed instead of risking a leak.
  if (!crewName) {
    return { status: 'error', error: 'Crew name not resolved; contact Tyler' };
  }

  // Compute current Mon-Sun pay period in America/New_York
  const nowEt = new Date(new Date().toLocaleString('en-US', { timeZone: 'America/New_York' }));
  const dow = nowEt.getDay();
  const daysSinceMon = (dow + 6) % 7;
  const monday = new Date(nowEt);
  monday.setDate(nowEt.getDate() - daysSinceMon);
  const sunday = new Date(monday);
  sunday.setDate(monday.getDate() + 6);
  const fmt = d => `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')}`;
  const weekStart = fmt(monday);
  const weekEnd = fmt(sunday);
  const fileName = `qbt_week_${weekStart}_${weekEnd}.xlsx`;
  const filePath = `Timiron/Claude/qbt_output/${fileName}`;

  const token = await getToken(env);
  const encFilePath = filePath.split('/').map(encodeURIComponent).join('/');
  const metaUrl = `${GRAPH_BASE}/me/drive/root:/${encFilePath}`;
  let fileId = null;
  try {
    const r = await fetch(metaUrl, { headers: { Authorization: `Bearer ${token}` } });
    if (r.status === 404) {
      return {
        status: 'ok',
        crew_name: crewName,
        week_start: weekStart,
        week_end: weekEnd,
        rows: [],
        total: 0,
        note: 'No timesheet published yet for this week.',
      };
    }
    if (!r.ok) return { status: 'error', error: `Graph metadata ${r.status}` };
    const meta = await r.json();
    fileId = meta.id;
  } catch (e) {
    return { status: 'error', error: 'Graph metadata fetch failed: ' + e.message };
  }

  let sheets = [];
  try {
    const wsResp = await fetch(`${GRAPH_BASE}/me/drive/items/${fileId}/workbook/worksheets`, {
      headers: { Authorization: `Bearer ${token}` },
    });
    if (wsResp.ok) {
      const wsData = await wsResp.json();
      sheets = (wsData.value || []).map(s => s.name);
    }
  } catch (e) { /* fall through */ }
  if (sheets.length === 0) sheets = ['Sheet1'];

  const matched = [];
  let weekTotal = 0;
  let usedSheet = null;
  let header = null;
  const lcName = crewName.toLowerCase().trim();
  for (const sheet of sheets) {
    const url = `${GRAPH_BASE}/me/drive/items/${fileId}/workbook/worksheets('${encodeURIComponent(sheet)}')/usedRange`;
    let rows;
    try {
      const r = await fetch(url, { headers: { Authorization: `Bearer ${token}` } });
      if (!r.ok) continue;
      const data = await r.json();
      rows = data.values || [];
    } catch (e) { continue; }
    if (!rows.length) continue;

    let foundInThisSheet = false;
    const candidateHeader = rows[0];
    for (let ri = 1; ri < rows.length; ri++) {
      const row = rows[ri] || [];
      let isMatch = false;
      for (let ci = 0; ci < row.length; ci++) {
        const cell = row[ci];
        if (typeof cell !== 'string') continue;
        if (cell.toLowerCase().trim() === lcName) { isMatch = true; break; }
      }
      if (isMatch) {
        matched.push(row);
        for (const v of row) {
          if (typeof v === 'number' && isFinite(v) && v >= 0 && v < 24) weekTotal += v;
        }
        foundInThisSheet = true;
      }
    }
    if (foundInThisSheet) {
      usedSheet = sheet;
      header = candidateHeader;
      break;
    }
  }

  return {
    status: 'ok',
    crew_name: crewName,
    week_start: weekStart,
    week_end: weekEnd,
    sheet: usedSheet,
    header: header,
    rows: matched,
    total: Math.round(weekTotal * 100) / 100,
    file: fileName,
  };
}

// ============================================================================
// FLAGMAN Task 6 — PDF + OneDrive + Email export on inspection submit
// ----------------------------------------------------------------------------
// pdf-lib version (bundled via wrangler/esbuild). Embeds photos pulled from
// R2 binding FLAGMAN_PHOTOS. ASCII-only Helvetica text, US Letter, multi-page.
// ============================================================================
import { PDFDocument, StandardFonts, rgb } from 'pdf-lib';

function asciiOnly(s) {
  if (s == null) return '';
  return String(s).replace(/[^\x09\x0A\x0D\x20-\x7E]/g, '');
}

// Word-wrap an ASCII string to a max char width (rough — pdf-lib measures
// pixel widths, but for monospaced-like Helvetica at 10pt, ~88 chars fits in
// ~504pt of usable width).
function wrapAscii(s, maxChars) {
  const out = [];
  const lines = asciiOnly(s).split(/\r?\n/);
  for (const line of lines) {
    if (line.length <= maxChars) { out.push(line); continue; }
    const words = line.split(/\s+/);
    let cur = '';
    for (const w of words) {
      if (!cur) { cur = w; continue; }
      if (cur.length + 1 + w.length <= maxChars) cur += ' ' + w;
      else { out.push(cur); cur = w; }
    }
    if (cur) out.push(cur);
  }
  return out;
}

// Detect image kind from first bytes. Returns 'jpg' | 'png' | null.
function detectImageKind(bytes) {
  if (!bytes || bytes.length < 8) return null;
  // JPEG: FF D8 FF
  if (bytes[0] === 0xFF && bytes[1] === 0xD8 && bytes[2] === 0xFF) return 'jpg';
  // PNG: 89 50 4E 47 0D 0A 1A 0A
  if (bytes[0] === 0x89 && bytes[1] === 0x50 && bytes[2] === 0x4E && bytes[3] === 0x47) return 'png';
  return null;
}

// Render the inspection as a multi-page PDF with embedded photos.
// Async because we fetch photo bytes from R2 (env.FLAGMAN_PHOTOS).
// Returns Uint8Array of PDF bytes.
async function renderInspectionPDF(record, env) {
  const pdfDoc = await PDFDocument.create();
  const font = await pdfDoc.embedFont(StandardFonts.Helvetica);
  const fontBold = await pdfDoc.embedFont(StandardFonts.HelveticaBold);

  const pageW = 612, pageH = 792;
  const marginL = 54, marginR = 54, marginTop = 750, marginBottom = 54;
  const usableW = pageW - marginL - marginR;
  const lineHeight = 13;

  let page = pdfDoc.addPage([pageW, pageH]);
  let y = marginTop;
  const black = rgb(0, 0, 0);
  const grey = rgb(0.4, 0.4, 0.4);

  function newPage() {
    page = pdfDoc.addPage([pageW, pageH]);
    y = marginTop;
  }

  function ensureRoom(needed) {
    if (y - needed < marginBottom) newPage();
  }

  function drawLine(text, opts = {}) {
    const f = opts.bold ? fontBold : font;
    const size = opts.size || 10;
    const color = opts.color || black;
    ensureRoom(size + 2);
    page.drawText(asciiOnly(text), { x: marginL, y: y - size, size, font: f, color });
    y -= (size + 3);
  }

  function drawWrapped(text, opts = {}) {
    const lines = wrapAscii(text, opts.maxChars || 88);
    for (const ln of lines) drawLine(ln, opts);
  }

  function blank(h) { y -= (h || lineHeight); }

  // Header
  drawLine('FLAGMAN Inspection Report', { bold: true, size: 16 });
  blank(4);
  drawLine(`Inspection ID: ${record.inspection_id || ''}`);
  drawLine(`Crew Token:    ${record.crew_token || ''}`);
  drawLine(`Type:          ${record.inspection_type || ''}`);
  drawLine(`Timestamp:     ${record.timestamp || ''}`);
  drawLine(`Received At:   ${record.received_at || ''}`);
  if (record.location) {
    const loc = typeof record.location === 'string' ? record.location : JSON.stringify(record.location);
    drawLine(`Location:      ${loc}`);
  }
  if (record.gps) {
    const gps = typeof record.gps === 'string' ? record.gps : JSON.stringify(record.gps);
    drawLine(`GPS:           ${gps}`);
  }
  blank();

  // Checklist (if present in record)
  const checklist = Array.isArray(record.checklist) ? record.checklist :
                    (record.checklist && typeof record.checklist === 'object'
                      ? Object.entries(record.checklist).map(([k, v]) => ({ item: k, state: v }))
                      : []);
  if (checklist.length) {
    drawLine('Checklist:', { bold: true, size: 11 });
    for (const c of checklist) {
      const item = (c && (c.item || c.name || c.label)) || '';
      const state = (c && (c.state || c.status || c.result || c.value)) || '';
      const note = (c && (c.note || c.notes || c.comment)) || '';
      const stateStr = String(state).toUpperCase();
      drawWrapped(`  [${stateStr}] ${item}${note ? ' -- ' + note : ''}`, { maxChars: 88 });
    }
    blank();
  }

  // Notes
  drawLine('Notes:', { bold: true, size: 11 });
  drawWrapped(record.notes ? String(record.notes) : '(none)', { maxChars: 88 });
  blank();

  // Photos
  const photos = Array.isArray(record.photos) ? record.photos : [];
  drawLine(`Photos (${photos.length}):`, { bold: true, size: 11 });

  // Embed up to 24 photos. Each rendered max 480w x 360h (preserves aspect).
  const maxEmbed = Math.min(photos.length, 24);
  const photoMaxW = 480, photoMaxH = 360;

  for (let i = 0; i < maxEmbed; i++) {
    const p = photos[i];
    const key = (typeof p === 'string') ? p : (p && (p.r2_key || p.key)) || '';
    if (!key) continue;
    let bytes = null;
    let imgErr = null;
    try {
      if (env && env.FLAGMAN_PHOTOS) {
        const obj = await env.FLAGMAN_PHOTOS.get(key);
        if (obj) bytes = new Uint8Array(await obj.arrayBuffer());
        else imgErr = 'not-found';
      } else {
        imgErr = 'no-binding';
      }
    } catch (e) {
      imgErr = 'fetch:' + (e.message || 'err');
    }

    // Caption (always rendered)
    drawLine(`  [${i + 1}] ${key}`, { color: grey, size: 9 });

    if (!bytes) {
      drawLine(`      (image unavailable: ${imgErr || 'unknown'})`, { color: grey, size: 9 });
      continue;
    }

    const kind = detectImageKind(bytes);
    if (!kind) {
      drawLine(`      (unsupported image format)`, { color: grey, size: 9 });
      continue;
    }

    let img;
    try {
      img = (kind === 'jpg') ? await pdfDoc.embedJpg(bytes) : await pdfDoc.embedPng(bytes);
    } catch (e) {
      drawLine(`      (embed failed: ${asciiOnly(e.message || 'err')})`, { color: grey, size: 9 });
      continue;
    }

    // Scale to fit photoMaxW x photoMaxH, preserve aspect
    const scaleW = photoMaxW / img.width;
    const scaleH = photoMaxH / img.height;
    const scale = Math.min(scaleW, scaleH, 1);
    const drawW = img.width * scale;
    const drawH = img.height * scale;

    ensureRoom(drawH + 8);
    page.drawImage(img, { x: marginL, y: y - drawH, width: drawW, height: drawH });
    y -= (drawH + 8);
  }
  if (photos.length > maxEmbed) {
    drawLine(`  ... +${photos.length - maxEmbed} more (not embedded)`, { color: grey, size: 9 });
  }

  // Footer on last page
  blank(8);
  drawLine(`Generated: ${new Date().toISOString()}`, { color: grey, size: 8 });
  drawLine(`Source:    cadiz-ops-worker (FLAGMAN Task 6, pdf-lib)`, { color: grey, size: 8 });

  return await pdfDoc.save();
}

function uint8ToBase64(bytes) {
  let bin = '';
  const chunk = 0x8000;
  for (let i = 0; i < bytes.length; i += chunk) {
    bin += String.fromCharCode.apply(null, bytes.subarray(i, i + chunk));
  }
  return btoa(bin);
}

async function exportInspectionToOneDriveAndEmail(record, env) {
  const id = record.inspection_id;
  if (!id) return { ok: false, error: 'missing inspection_id' };

  const exportedKey = `flagman:exported:${id}`;
  const already = await env.KV.get(exportedKey);
  if (already === '1') return { ok: true, skipped: 'already_exported' };

  let token;
  try {
    token = await getToken(env);
  } catch (e) {
    await env.KV.put(`flagman:export_error:${id}`, JSON.stringify({ stage: 'token', error: e.message, at: new Date().toISOString() }), { expirationTtl: 7 * 86400 });
    return { ok: false, error: `token: ${e.message}` };
  }

  const date = (record.timestamp || new Date().toISOString()).split('T')[0];
  const crewSlug = (record.crew_token || 'unknown').replace(/[^a-zA-Z0-9_-]/g, '_');
  const baseDir = `Timiron/_flagman/${date}`;
  const baseName = `${crewSlug}-${id}`;
  const jsonPath = `/me/drive/root:/${baseDir}/${baseName}.json:/content`;
  const pdfPath = `/me/drive/root:/${baseDir}/${baseName}.pdf:/content`;

  // 1. PUT JSON
  let jsonStatus = 0, jsonItem = null;
  try {
    const r = await fetch(`${GRAPH_BASE}${jsonPath}`, {
      method: 'PUT',
      headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
      body: JSON.stringify(record, null, 2),
    });
    jsonStatus = r.status;
    if (r.ok) jsonItem = await r.json();
  } catch (e) {
    await env.KV.put(`flagman:export_error:${id}`, JSON.stringify({ stage: 'json_put', error: e.message }), { expirationTtl: 7 * 86400 });
    return { ok: false, error: `json_put: ${e.message}` };
  }
  if (!jsonItem) {
    await env.KV.put(`flagman:export_error:${id}`, JSON.stringify({ stage: 'json_put', status: jsonStatus }), { expirationTtl: 7 * 86400 });
    return { ok: false, error: `json_put status ${jsonStatus}` };
  }

  // 2. Render + PUT PDF
  let pdfBytes;
  try {
    pdfBytes = await renderInspectionPDF(record, env);
  } catch (e) {
    await env.KV.put(`flagman:export_error:${id}`, JSON.stringify({ stage: 'pdf_render', error: e.message }), { expirationTtl: 7 * 86400 });
    return { ok: false, error: `pdf_render: ${e.message}` };
  }

  let pdfStatus = 0, pdfItem = null;
  try {
    const r = await fetch(`${GRAPH_BASE}${pdfPath}`, {
      method: 'PUT',
      headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/pdf' },
      body: pdfBytes,
    });
    pdfStatus = r.status;
    if (r.ok) pdfItem = await r.json();
  } catch (e) {
    await env.KV.put(`flagman:export_error:${id}`, JSON.stringify({ stage: 'pdf_put', error: e.message }), { expirationTtl: 7 * 86400 });
    return { ok: false, error: `pdf_put: ${e.message}` };
  }
  if (!pdfItem) {
    await env.KV.put(`flagman:export_error:${id}`, JSON.stringify({ stage: 'pdf_put', status: pdfStatus }), { expirationTtl: 7 * 86400 });
    return { ok: false, error: `pdf_put status ${pdfStatus}` };
  }

  // 3. Email Tyler with PDF attached
  const recipient = 'tylerk@timironmp.com';
  const subject = asciiOnly(`FLAGMAN inspection ${id} - ${crewSlug} - ${date}`);
  const photoCount = Array.isArray(record.photos) ? record.photos.length : 0;
  const bodyLines = [
    `FLAGMAN inspection submitted.`,
    ``,
    `Inspection ID: ${id}`,
    `Crew:          ${record.crew_token || ''}`,
    `Type:          ${record.inspection_type || ''}`,
    `Timestamp:     ${record.timestamp || ''}`,
    `Photos:        ${photoCount}`,
    ``,
    `OneDrive PDF:  ${pdfItem.webUrl || baseDir + '/' + baseName + '.pdf'}`,
    `OneDrive JSON: ${jsonItem.webUrl || baseDir + '/' + baseName + '.json'}`,
    ``,
    `Notes:`,
    asciiOnly(record.notes || '(none)'),
    ``,
    `-- cadiz-ops-worker (FLAGMAN Task 6)`,
  ];
  const bodyText = asciiOnly(bodyLines.join('\n'));
  const pdfB64 = uint8ToBase64(pdfBytes);
  const mailPayload = {
    message: {
      subject: subject,
      body: { contentType: 'Text', content: bodyText },
      toRecipients: [{ emailAddress: { address: recipient } }],
      attachments: [{
        '@odata.type': '#microsoft.graph.fileAttachment',
        name: `${baseName}.pdf`,
        contentType: 'application/pdf',
        contentBytes: pdfB64,
      }],
    },
    saveToSentItems: 'true',
  };
  let mailStatus = 0, mailErr = null;
  try {
    const r = await fetch(`${GRAPH_BASE}/me/sendMail`, {
      method: 'POST',
      headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
      body: JSON.stringify(mailPayload),
    });
    mailStatus = r.status;
    if (!r.ok) {
      try { mailErr = await r.text(); } catch (_) { mailErr = `status ${r.status}`; }
    }
  } catch (e) {
    mailErr = e.message;
  }
  if (mailErr) {
    await env.KV.put(`flagman:export_error:${id}`, JSON.stringify({ stage: 'sendmail', status: mailStatus, error: mailErr }), { expirationTtl: 7 * 86400 });
    return {
      ok: false,
      stage: 'sendmail',
      json_url: jsonItem.webUrl,
      pdf_url: pdfItem.webUrl,
      error: mailErr,
    };
  }

  // 4. Mark exported
  await env.KV.put(exportedKey, '1', { expirationTtl: 7 * 86400 });
  await env.KV.delete(`flagman:export_error:${id}`);

  return {
    ok: true,
    inspection_id: id,
    json_url: jsonItem.webUrl,
    pdf_url: pdfItem.webUrl,
    pdf_size: pdfBytes.length,
    mail_status: mailStatus,
    sent_to: recipient,
  };
}

// ============================================================================
// FLAGMAN OCR support handlers (Task: pending-ocr list + OCR writeback)
// ----------------------------------------------------------------------------
// handleFlagmanPendingOcr({ token, limit }, env)
//   → { status, body }  where body = { count, pending: [{inspection_id, frame_keys}] }
//
// handleFlagmanOcrWriteback({ token, id, ocr }, env)
//   → { status, body }  where body = { status:'ok', inspection_id } | { error }
//
// Admin gate: same pattern as /api/flagman/admin/* — ?token resolves to role=admin
// via training-portal.  Returns status 403 on missing/bad token (mirrors existing).
// ============================================================================

async function _flagmanAdminAuth(token) {
  if (!token) return false;
  try {
    const r = await fetch(`https://training.kolassus.ai/api/crew/${encodeURIComponent(token)}`, {
      headers: { 'User-Agent': 'FLAGMAN/1.0' },
    });
    if (r.status === 200) {
      const j = await r.json();
      if (j && j.crew && j.crew.role === 'admin') return true;
    }
  } catch (_) { /* fall through */ }
  return false;
}

async function handleFlagmanPendingOcr({ token, limit }, env) {
  const adminOk = await _flagmanAdminAuth((token || '').trim());
  if (!adminOk) {
    return { status: 403, body: { error: 'forbidden' } };
  }

  const cap = (limit !== null && limit !== undefined && !isNaN(Number(limit)) && Number(limit) > 0)
    ? Math.floor(Number(limit))
    : 10;

  let recent = [];
  try {
    const raw = await env.KV.get('flagman:inspections:recent');
    if (raw) recent = JSON.parse(raw);
    if (!Array.isArray(recent)) recent = [];
  } catch (_) { recent = []; }

  const pending = [];
  for (const summary of recent) {
    if (pending.length >= cap) break;
    // Skip entries already marked as has_ocr in the summary (fast path)
    if (summary.has_ocr) continue;
    // Fetch full record to check frame_keys and ocr block
    let record = null;
    try {
      const recRaw = await env.KV.get(`flagman:inspection:${summary.inspection_id}`);
      if (!recRaw) continue;
      record = JSON.parse(recRaw);
    } catch (_) { continue; }
    if (!Array.isArray(record.frame_keys) || record.frame_keys.length === 0) continue;
    if (record.ocr) continue;
    pending.push({ inspection_id: record.inspection_id, frame_keys: record.frame_keys });
  }

  return { status: 200, body: { count: pending.length, pending } };
}

async function handleFlagmanOcrWriteback({ token, id, ocr }, env) {
  const adminOk = await _flagmanAdminAuth((token || '').trim());
  if (!adminOk) {
    return { status: 403, body: { error: 'forbidden' } };
  }

  const recKey = `flagman:inspection:${id}`;
  let record = null;
  try {
    const raw = await env.KV.get(recKey);
    if (!raw) return { status: 404, body: { error: 'not_found' } };
    record = JSON.parse(raw);
  } catch (_) {
    return { status: 404, body: { error: 'not_found' } };
  }

  // Attach/overwrite ocr block; set _at if not provided
  const ocrBlock = Object.assign({}, ocr);
  if (!ocrBlock._at) ocrBlock._at = new Date().toISOString();
  record.ocr = ocrBlock;

  await env.KV.put(recKey, JSON.stringify(record));

  // Reflect has_ocr in the recent summary list (additive, back-compat)
  try {
    const recentRaw = await env.KV.get('flagman:inspections:recent');
    if (recentRaw) {
      let recent = JSON.parse(recentRaw);
      if (Array.isArray(recent)) {
        let updated = false;
        for (const s of recent) {
          if (s.inspection_id === id) { s.has_ocr = true; updated = true; break; }
        }
        if (updated) await env.KV.put('flagman:inspections:recent', JSON.stringify(recent));
      }
    }
  } catch (_) { /* non-fatal — writeback already committed */ }

  return { status: 200, body: { status: 'ok', inspection_id: id } };
}

// Test-only export (tree-shaken in production build)
export { handleFlagmanSubmit, handleFlagmanPendingOcr, handleFlagmanOcrWriteback };
