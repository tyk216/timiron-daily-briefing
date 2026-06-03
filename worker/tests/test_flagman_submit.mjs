// test_flagman_submit.mjs — unit tests for handleFlagmanSubmit (rich payload + idempotency)
// Run: node worker/tests/test_flagman_submit.mjs
//
// Approach: handleFlagmanSubmit is a pure async function that takes (payload, env).
// We mock env.KV (Map-backed) and env.fetch (replaced on globalThis) to avoid any
// Cloudflare runtime dependency. The function is imported from the worker source
// as a named export added for testing.

import { handleFlagmanSubmit } from '../src/index.js';

let pass = 0, fail = 0;
function assert(cond, msg) {
  if (cond) { pass++; console.log('OK  ', msg); }
  else       { fail++; console.error('FAIL', msg); process.exitCode = 1; }
}

// ---------------------------------------------------------------------------
// KV mock — flat Map, mirrors Cloudflare KV interface used by the handler
// ---------------------------------------------------------------------------
function makeKV(seed = {}) {
  const store = new Map(Object.entries(seed));
  return {
    async get(k)             { return store.has(k) ? store.get(k) : null; },
    async put(k, v, _opts)   { store.set(k, v); },
    async delete(k)          { store.delete(k); },
    // test helper
    _store: store,
  };
}

// ---------------------------------------------------------------------------
// Fetch mock factory — controls training-portal responses
// ---------------------------------------------------------------------------
function makeFetch(statusCode, body = {}) {
  return async (_url, _opts) => ({
    status: statusCode,
    async json() { return body; },
  });
}

// ---------------------------------------------------------------------------
// Helper: build a minimal valid env with KV + patched fetch
// ---------------------------------------------------------------------------
function makeEnv({ kvSeed = {}, trainingStatus = 200, trainingBody = { name: 'Test Crew' } } = {}) {
  const kv = makeKV(kvSeed);
  const env = { KV: kv };
  // Patch global fetch for this call — handler calls fetch() directly
  globalThis.fetch = makeFetch(trainingStatus, trainingBody);
  return env;
}

// ---------------------------------------------------------------------------
// Test 1: Rich payload stored; flat photos = union(items photo_keys, frame_keys), de-duped
// ---------------------------------------------------------------------------
{
  const env = makeEnv();
  const payload = {
    crew_token: 'tok-abc',
    asset: 'Railcar',
    checklist_id: 'rail-arrival-v1',
    direction: 'arrival',
    railcar_number: 'PROX45141',
    source: 'pwa',
    items: [
      { id: 'item1', value: 'ok', notes: '', photo_keys: ['r2/photo-a', 'r2/photo-b'] },
      { id: 'item2', value: 'fail', notes: 'crack', photo_keys: ['r2/photo-b', 'r2/photo-c'] },
    ],
    frame_keys: ['r2/frame-1', 'r2/photo-a'],  // photo-a is a dupe
    photos: ['r2/legacy-1'],
    notes: 'test notes',
    submission_id: 'sub-001',
  };

  const result = await handleFlagmanSubmit(payload, env);
  assert(result.status === 'ok', 'T1: status ok');
  assert(typeof result.inspection_id === 'string', 'T1: inspection_id returned');

  const raw = env.KV._store.get(`flagman:inspection:${result.inspection_id}`);
  assert(raw != null, 'T1: record written to KV');
  const record = JSON.parse(raw);

  // photos = de-duped union of legacy + items.photo_keys + frame_keys
  // expected: r2/legacy-1, r2/photo-a, r2/photo-b, r2/photo-c, r2/frame-1  (5 unique)
  assert(record.photos.length === 5, `T1: photos union de-duped to 5 (got ${record.photos.length})`);
  assert(record.photos.includes('r2/photo-a'), 'T1: photo-a present');
  assert(record.photos.includes('r2/photo-b'), 'T1: photo-b present');
  assert(record.photos.includes('r2/photo-c'), 'T1: photo-c present');
  assert(record.photos.includes('r2/frame-1'), 'T1: frame-1 present');
  assert(record.photos.includes('r2/legacy-1'), 'T1: legacy-1 present');

  // Rich fields stored
  assert(record.asset === 'Railcar', 'T1: asset stored');
  assert(record.railcar_number === 'PROX45141', 'T1: railcar_number stored');
  assert(record.direction === 'arrival', 'T1: direction stored');
  assert(record.source === 'pwa', 'T1: source stored');
  assert(record.checklist_id === 'rail-arrival-v1', 'T1: checklist_id stored');
  assert(Array.isArray(record.items) && record.items.length === 2, 'T1: items stored');
  assert(record.submission_id === 'sub-001', 'T1: submission_id stored');

  // summary pushed to recent
  const recentRaw = env.KV._store.get('flagman:inspections:recent');
  assert(recentRaw != null, 'T1: recent list written');
  const recent = JSON.parse(recentRaw);
  assert(recent.length === 1, 'T1: one entry in recent');
  assert(recent[0].asset === 'Railcar', 'T1: recent summary includes asset');
  assert(recent[0].railcar_number === 'PROX45141', 'T1: recent summary includes railcar_number');
  assert(recent[0].inspection_type === 'rail-arrival-v1', 'T1: recent summary inspection_type from checklist_id');
}

// ---------------------------------------------------------------------------
// Test 2: inspection_type defaults to checklist_id when inspection_type absent
// ---------------------------------------------------------------------------
{
  const env = makeEnv();
  const payload = {
    crew_token: 'tok-abc',
    checklist_id: 'daily-walkaround',
    notes: '',
  };
  const result = await handleFlagmanSubmit(payload, env);
  assert(result.status === 'ok', 'T2: status ok');
  const record = JSON.parse(env.KV._store.get(`flagman:inspection:${result.inspection_id}`));
  assert(record.inspection_type === 'daily-walkaround', 'T2: inspection_type defaults to checklist_id');
}

// ---------------------------------------------------------------------------
// Test 3: inspection_type defaults to "daily" when neither field provided
// ---------------------------------------------------------------------------
{
  const env = makeEnv();
  const payload = { crew_token: 'tok-abc', notes: 'minimal' };
  const result = await handleFlagmanSubmit(payload, env);
  assert(result.status === 'ok', 'T3: status ok');
  const record = JSON.parse(env.KV._store.get(`flagman:inspection:${result.inspection_id}`));
  assert(record.inspection_type === 'daily', 'T3: inspection_type defaults to "daily"');
}

// ---------------------------------------------------------------------------
// Test 4: Duplicate submission_id returns same inspection_id, no new recent entry
// ---------------------------------------------------------------------------
{
  const env = makeEnv();
  const payload = {
    crew_token: 'tok-abc',
    submission_id: 'sub-idem-1',
    notes: 'first submit',
  };

  // First submit
  const r1 = await handleFlagmanSubmit(payload, env);
  assert(r1.status === 'ok', 'T4: first submit ok');
  const firstId = r1.inspection_id;
  const recentAfterFirst = JSON.parse(env.KV._store.get('flagman:inspections:recent'));
  assert(recentAfterFirst.length === 1, 'T4: one entry after first submit');

  // Reset fetch mock (token cache means fetch won't fire again, but just in case)
  globalThis.fetch = makeFetch(200, { name: 'Test Crew' });

  // Second submit with same submission_id
  const r2 = await handleFlagmanSubmit({ ...payload, notes: 'replay attempt' }, env);
  assert(r2.status === 'ok', 'T4: duplicate submit returns ok');
  assert(r2.inspection_id === firstId, `T4: same inspection_id returned (got ${r2.inspection_id}, want ${firstId})`);

  // recent list must NOT have grown
  const recentAfterSecond = JSON.parse(env.KV._store.get('flagman:inspections:recent'));
  assert(recentAfterSecond.length === 1, `T4: recent list still has 1 entry (no duplicate push) — got ${recentAfterSecond.length}`);
}

// ---------------------------------------------------------------------------
// Test 5: Token validation rejects invalid token (training-portal 404)
// ---------------------------------------------------------------------------
{
  const env = makeEnv({ trainingStatus: 404 });
  const payload = { crew_token: 'bad-token', notes: 'should fail' };
  const result = await handleFlagmanSubmit(payload, env);
  assert(result.status === 'error', 'T5: invalid token rejected');
  assert(result.error === 'Invalid crew token', `T5: correct error message (got "${result.error}")`);
  // No inspection record written
  let anyInspection = false;
  for (const k of env.KV._store.keys()) {
    if (k.startsWith('flagman:inspection:')) anyInspection = true;
  }
  assert(!anyInspection, 'T5: no inspection record written for invalid token');
}

// ---------------------------------------------------------------------------
// Test 6: Token validation rejects on upstream error (non-200/404)
// ---------------------------------------------------------------------------
{
  const env = makeEnv({ trainingStatus: 503 });
  const payload = { crew_token: 'tok-upstream-err', notes: '' };
  const result = await handleFlagmanSubmit(payload, env);
  assert(result.status === 'error', 'T6: upstream error returns error');
  assert(result.error === 'Token validation upstream error', `T6: upstream error message (got "${result.error}")`);
}

// ---------------------------------------------------------------------------
// Test 7: Back-compat — explicit inspection_type wins over checklist_id
// ---------------------------------------------------------------------------
{
  const env = makeEnv();
  const payload = {
    crew_token: 'tok-abc',
    inspection_type: 'pre-shift',
    checklist_id: 'daily-walkaround',
    notes: '',
  };
  const result = await handleFlagmanSubmit(payload, env);
  assert(result.status === 'ok', 'T7: status ok');
  const record = JSON.parse(env.KV._store.get(`flagman:inspection:${result.inspection_id}`));
  assert(record.inspection_type === 'pre-shift', 'T7: explicit inspection_type wins over checklist_id');
}

// ---------------------------------------------------------------------------
// Test 8: Token cached as valid skips fetch; second call resolves correctly
// ---------------------------------------------------------------------------
{
  // Pre-seed KV with a valid token cache entry
  const env = makeEnv({ kvSeed: { 'flagman:token_valid:cached-tok': '1' } });
  // Fetch would return 404, but should NOT be called (token already cached)
  globalThis.fetch = makeFetch(404);

  const payload = { crew_token: 'cached-tok', notes: 'cached token test' };
  const result = await handleFlagmanSubmit(payload, env);
  assert(result.status === 'ok', 'T8: cached valid token bypasses fetch');
}

// ---------------------------------------------------------------------------
console.log(`\n${pass} pass, ${fail} fail`);
if (fail > 0) process.exit(1);
