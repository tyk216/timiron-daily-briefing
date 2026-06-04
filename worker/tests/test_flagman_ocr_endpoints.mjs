// test_flagman_ocr_endpoints.mjs — TDD tests for two FLAGMAN OCR endpoints:
//   GET  /api/flagman/pending-ocr?token=<adminToken>&limit=<N>
//   POST /api/flagman/inspection/<id>/ocr?token=<adminToken>
//
// Run: node --test "worker/tests/test_flagman_ocr_endpoints.mjs"
//      OR from worker/: node --test "tests/test_flagman_ocr_endpoints.mjs"
//
// Approach: handlers are pure async functions exported for testing.
// We mock env.KV (Map-backed), env.fetch (globalThis replacement), and corsHeaders.

import { strict as assert } from 'node:assert';
import { describe, it, before, after } from 'node:test';
import { handleFlagmanPendingOcr, handleFlagmanOcrWriteback } from '../src/index.js';

// ---------------------------------------------------------------------------
// KV mock — flat Map, mirrors Cloudflare KV interface
// ---------------------------------------------------------------------------
function makeKV(seed = {}) {
  const store = new Map(Object.entries(seed));
  return {
    async get(k)           { return store.has(k) ? store.get(k) : null; },
    async put(k, v, _opts) { store.set(k, v); },
    async delete(k)        { store.delete(k); },
    _store: store,
  };
}

// ---------------------------------------------------------------------------
// Fetch mock — controls training-portal /api/crew/<token> responses
// ---------------------------------------------------------------------------
function makeAdminFetch(adminToken) {
  return async (url, _opts) => {
    const u = typeof url === 'string' ? url : url.toString();
    if (u.includes('/api/crew/')) {
      const tok = decodeURIComponent(u.split('/api/crew/')[1]);
      if (tok === adminToken) {
        return {
          status: 200,
          json: async () => ({ crew: { role: 'admin', name: 'Tyler' } }),
        };
      }
      return {
        status: 403,
        json: async () => ({ error: 'forbidden' }),
      };
    }
    return { status: 404, json: async () => ({}) };
  };
}

const ADMIN_TOKEN = 'tyler-kolaczynski';
const BAD_TOKEN   = 'not-an-admin';

// ---------------------------------------------------------------------------
// Seed helpers
// ---------------------------------------------------------------------------
function makeInspectionRecord(id, opts = {}) {
  return {
    inspection_id: id,
    crew_token: 'crew-abc',
    crew_name: 'Test Crew',
    inspection_type: 'daily',
    timestamp: new Date().toISOString(),
    photos: [],
    frame_keys: opts.frame_keys ?? [],
    received_at: new Date().toISOString(),
    ...(opts.ocr ? { ocr: opts.ocr } : {}),
  };
}

function makeSummary(id, opts = {}) {
  return {
    inspection_id: id,
    crew_token: 'crew-abc',
    crew_name: 'Test Crew',
    inspection_type: 'daily',
    timestamp: new Date().toISOString(),
    photo_count: 0,
    location: null,
    asset: null,
    railcar_number: null,
    ...(opts.has_ocr ? { has_ocr: true } : {}),
  };
}

// ---------------------------------------------------------------------------
// SUITE 1 — GET /api/flagman/pending-ocr
// ---------------------------------------------------------------------------
describe('GET /api/flagman/pending-ocr', () => {
  let savedFetch;
  before(() => { savedFetch = globalThis.fetch; globalThis.fetch = makeAdminFetch(ADMIN_TOKEN); });
  after(() => { globalThis.fetch = savedFetch; });

  it('returns 403 when token is missing', async () => {
    const kv = makeKV();
    const result = await handleFlagmanPendingOcr({ token: '', limit: 10 }, { KV: kv });
    assert.equal(result.status, 403);
  });

  it('returns 403 when token does not resolve to admin role', async () => {
    const kv = makeKV();
    const result = await handleFlagmanPendingOcr({ token: BAD_TOKEN, limit: 10 }, { KV: kv });
    assert.equal(result.status, 403);
  });

  it('returns empty list when no inspections exist', async () => {
    const kv = makeKV(); // no flagman:inspections:recent
    const result = await handleFlagmanPendingOcr({ token: ADMIN_TOKEN, limit: 10 }, { KV: kv });
    assert.equal(result.status, 200);
    assert.deepEqual(result.body, { count: 0, pending: [] });
  });

  it('returns only inspections that have frame_keys and no ocr block', async () => {
    const id1 = 'ins-has-frames-no-ocr';
    const id2 = 'ins-no-frames';
    const id3 = 'ins-has-frames-with-ocr';

    const rec1 = makeInspectionRecord(id1, { frame_keys: ['frame/a.jpg', 'frame/b.jpg'] });
    const rec2 = makeInspectionRecord(id2, { frame_keys: [] });
    const rec3 = makeInspectionRecord(id3, { frame_keys: ['frame/c.jpg'], ocr: { reporting_marks: 'CSXT', _at: new Date().toISOString() } });

    const recent = [makeSummary(id1), makeSummary(id2), makeSummary(id3, { has_ocr: true })];
    const kv = makeKV({
      'flagman:inspections:recent': JSON.stringify(recent),
      [`flagman:inspection:${id1}`]: JSON.stringify(rec1),
      [`flagman:inspection:${id2}`]: JSON.stringify(rec2),
      [`flagman:inspection:${id3}`]: JSON.stringify(rec3),
    });

    const result = await handleFlagmanPendingOcr({ token: ADMIN_TOKEN, limit: 10 }, { KV: kv });
    assert.equal(result.status, 200);
    assert.equal(result.body.count, 1);
    assert.equal(result.body.pending[0].inspection_id, id1);
    assert.deepEqual(result.body.pending[0].frame_keys, ['frame/a.jpg', 'frame/b.jpg']);
  });

  it('respects limit parameter', async () => {
    const ids = ['i1', 'i2', 'i3', 'i4', 'i5'];
    const recent = ids.map(id => makeSummary(id));
    const kvSeed = { 'flagman:inspections:recent': JSON.stringify(recent) };
    for (const id of ids) {
      kvSeed[`flagman:inspection:${id}`] = JSON.stringify(makeInspectionRecord(id, { frame_keys: ['f.jpg'] }));
    }
    const kv = makeKV(kvSeed);

    const result = await handleFlagmanPendingOcr({ token: ADMIN_TOKEN, limit: 2 }, { KV: kv });
    assert.equal(result.status, 200);
    assert.equal(result.body.count, 2);
    assert.equal(result.body.pending.length, 2);
  });

  it('uses default limit of 10 when limit not specified', async () => {
    const ids = Array.from({ length: 15 }, (_, i) => `ins-${i}`);
    const recent = ids.map(id => makeSummary(id));
    const kvSeed = { 'flagman:inspections:recent': JSON.stringify(recent) };
    for (const id of ids) {
      kvSeed[`flagman:inspection:${id}`] = JSON.stringify(makeInspectionRecord(id, { frame_keys: ['f.jpg'] }));
    }
    const kv = makeKV(kvSeed);

    const result = await handleFlagmanPendingOcr({ token: ADMIN_TOKEN, limit: null }, { KV: kv });
    assert.equal(result.status, 200);
    assert.equal(result.body.pending.length, 10);
  });

  it('skips inspections where full record is missing from KV', async () => {
    const id = 'orphan-summary';
    const recent = [makeSummary(id)];
    // No flagman:inspection:<id> key in KV
    const kv = makeKV({ 'flagman:inspections:recent': JSON.stringify(recent) });

    const result = await handleFlagmanPendingOcr({ token: ADMIN_TOKEN, limit: 10 }, { KV: kv });
    assert.equal(result.status, 200);
    assert.equal(result.body.count, 0);
  });
});

// ---------------------------------------------------------------------------
// SUITE 2 — POST /api/flagman/inspection/<id>/ocr
// ---------------------------------------------------------------------------
describe('POST /api/flagman/inspection/<id>/ocr', () => {
  let savedFetch;
  before(() => { savedFetch = globalThis.fetch; globalThis.fetch = makeAdminFetch(ADMIN_TOKEN); });
  after(() => { globalThis.fetch = savedFetch; });

  it('returns 403 when token is missing', async () => {
    const kv = makeKV();
    const result = await handleFlagmanOcrWriteback({ token: '', id: 'any-id', ocr: {} }, { KV: kv });
    assert.equal(result.status, 403);
  });

  it('returns 403 when token does not resolve to admin role', async () => {
    const kv = makeKV();
    const result = await handleFlagmanOcrWriteback({ token: BAD_TOKEN, id: 'any-id', ocr: {} }, { KV: kv });
    assert.equal(result.status, 403);
  });

  it('returns 404 when inspection record does not exist', async () => {
    const kv = makeKV(); // no record
    const result = await handleFlagmanOcrWriteback({ token: ADMIN_TOKEN, id: 'missing-id', ocr: { reporting_marks: 'CSXT' } }, { KV: kv });
    assert.equal(result.status, 404);
  });

  it('attaches ocr block to the inspection record', async () => {
    const id = 'ins-to-ocr';
    const rec = makeInspectionRecord(id, { frame_keys: ['frame/x.jpg'] });
    const kv = makeKV({ [`flagman:inspection:${id}`]: JSON.stringify(rec) });

    const ocrPayload = { reporting_marks: 'CSXT', car_number: '123456', _confidence: { reporting_marks: 0.95 } };
    const result = await handleFlagmanOcrWriteback({ token: ADMIN_TOKEN, id, ocr: ocrPayload }, { KV: kv });

    assert.equal(result.status, 200);
    assert.deepEqual(result.body, { status: 'ok', inspection_id: id });

    // Verify persisted record has ocr block
    const stored = JSON.parse(kv._store.get(`flagman:inspection:${id}`));
    assert.equal(stored.ocr.reporting_marks, 'CSXT');
    assert.equal(stored.ocr.car_number, '123456');
    assert.ok(stored.ocr._at, '_at timestamp should be set');
  });

  it('sets _at timestamp if not provided in ocr payload', async () => {
    const id = 'ins-no-at';
    const rec = makeInspectionRecord(id, { frame_keys: ['frame/y.jpg'] });
    const kv = makeKV({ [`flagman:inspection:${id}`]: JSON.stringify(rec) });

    await handleFlagmanOcrWriteback({ token: ADMIN_TOKEN, id, ocr: { reporting_marks: 'UP' } }, { KV: kv });

    const stored = JSON.parse(kv._store.get(`flagman:inspection:${id}`));
    assert.ok(stored.ocr._at, '_at should be auto-set');
    assert.ok(!isNaN(new Date(stored.ocr._at).getTime()), '_at should be valid ISO date');
  });

  it('preserves _at if provided by caller', async () => {
    const id = 'ins-caller-at';
    const rec = makeInspectionRecord(id, { frame_keys: ['frame/z.jpg'] });
    const kv = makeKV({ [`flagman:inspection:${id}`]: JSON.stringify(rec) });

    const callerAt = '2026-06-01T12:00:00.000Z';
    await handleFlagmanOcrWriteback({ token: ADMIN_TOKEN, id, ocr: { reporting_marks: 'NS', _at: callerAt } }, { KV: kv });

    const stored = JSON.parse(kv._store.get(`flagman:inspection:${id}`));
    assert.equal(stored.ocr._at, callerAt);
  });

  it('is idempotent — re-POST overwrites the ocr block', async () => {
    const id = 'ins-idempotent';
    const rec = makeInspectionRecord(id, { frame_keys: ['frame/a.jpg'] });
    const kv = makeKV({ [`flagman:inspection:${id}`]: JSON.stringify(rec) });

    await handleFlagmanOcrWriteback({ token: ADMIN_TOKEN, id, ocr: { reporting_marks: 'BNSF' } }, { KV: kv });
    await handleFlagmanOcrWriteback({ token: ADMIN_TOKEN, id, ocr: { reporting_marks: 'CN', car_number: '999' } }, { KV: kv });

    const stored = JSON.parse(kv._store.get(`flagman:inspection:${id}`));
    assert.equal(stored.ocr.reporting_marks, 'CN');
    assert.equal(stored.ocr.car_number, '999');
  });

  it('after writeback, pending-ocr no longer lists the inspection', async () => {
    const id = 'ins-written-back';
    const rec = makeInspectionRecord(id, { frame_keys: ['frame/w.jpg'] });
    const recent = [makeSummary(id)];
    const kv = makeKV({
      'flagman:inspections:recent': JSON.stringify(recent),
      [`flagman:inspection:${id}`]: JSON.stringify(rec),
    });

    // Before writeback — should be pending
    const before = await handleFlagmanPendingOcr({ token: ADMIN_TOKEN, limit: 10 }, { KV: kv });
    assert.equal(before.body.count, 1);

    // Write OCR
    await handleFlagmanOcrWriteback({ token: ADMIN_TOKEN, id, ocr: { reporting_marks: 'KCS' } }, { KV: kv });

    // After writeback — should not be pending
    const after = await handleFlagmanPendingOcr({ token: ADMIN_TOKEN, limit: 10 }, { KV: kv });
    assert.equal(after.body.count, 0);
  });

  it('reflects has_ocr:true in the recent summary list', async () => {
    const id = 'ins-summary-flag';
    const rec = makeInspectionRecord(id, { frame_keys: ['frame/s.jpg'] });
    const recent = [makeSummary(id)];
    const kv = makeKV({
      'flagman:inspections:recent': JSON.stringify(recent),
      [`flagman:inspection:${id}`]: JSON.stringify(rec),
    });

    await handleFlagmanOcrWriteback({ token: ADMIN_TOKEN, id, ocr: { reporting_marks: 'CSX' } }, { KV: kv });

    const updatedRecent = JSON.parse(kv._store.get('flagman:inspections:recent'));
    const entry = updatedRecent.find(s => s.inspection_id === id);
    assert.ok(entry, 'summary entry should still exist');
    assert.equal(entry.has_ocr, true);
  });
});
