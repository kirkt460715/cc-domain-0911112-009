import test from 'node:test';
import assert from 'node:assert/strict';
import {setup, row, batch, code} from './helpers.js';
import {listen} from '../src/http.js';
import {execute, argumentsFor} from '../src/cli.js';
import {rollingSummary, dailyCoverage, thresholdRuns} from '../src/resampling.js';
import {digest} from '../src/value.js';

test('pagination is stable until a write invalidates the cursor', async t => {
  const {ledger} = await setup(t);
  await ledger.ingest(batch([row(), row({time: '2026-11-01T01:00:00Z'}), row({time: '2026-11-01T02:00:00Z'})]));
  const first = await ledger.query({limit: 1});
  assert.equal(first.total, 3);
  assert.equal(first.rows.length, 1);
  assert.ok(first.next);
  const second = await ledger.query({limit: 1, cursor: first.next});
  assert.equal(second.rows[0].time, '2026-11-01T01:00:00.000Z');
  const third = await ledger.query({limit: 1, cursor: second.next});
  assert.equal(third.next, null);
  assert.equal(third.rows[0].time, '2026-11-01T02:00:00.000Z');
  await assert.rejects(ledger.query({limit: 2, cursor: first.next}), code('CURSOR_QUERY'));
  await ledger.updateSource('s1', {priority: 2});
  await assert.rejects(ledger.query({limit: 1, cursor: first.next}), code('CURSOR_STALE'));
  await assert.rejects(ledger.query({cursor: 'garbage'}), code('CURSOR'));
});

test('export preserves immutable snapshot and reports changed observations', async t => {
  const {ledger} = await setup(t);
  await ledger.ingest(batch());
  const created = await ledger.export({id: 'e1', query: {}, purpose: 'synthetic analysis'});
  const content = created.export.content;
  assert.equal(created.export.count, 1);
  assert.equal(digest(content), created.export.checksum);
  assert.equal((await ledger.export({id: 'e1', query: {}, purpose: 'synthetic analysis'})).replay, true);
  await ledger.ingest(batch([row({revision: 2, value: 3})], {id: 'b2'}));
  const fetched = await ledger.getExport('e1');
  assert.equal(fetched.stale, true);
  assert.equal(fetched.content, content);
  assert.equal(fetched.changed[0].currentRevision, 2);
  assert.equal(fetched.changed[0].exportedRevision, 1);
  const manifest = await ledger.exportManifest('e1');
  assert.equal(manifest.bytes, Buffer.byteLength(content));
  assert.equal(Object.hasOwn(manifest, 'content'), false);
  await assert.rejects(ledger.export({id: 'e1', query: {}, purpose: 'synthetic analysis'}), code('EXPORT_CONFLICT'));
});

test('revocation prevents retrieval and immutable export id cannot be reused', async t => {
  const {ledger} = await setup(t);
  await ledger.ingest(batch());
  await ledger.export({id: 'e1', query: {}, format: 'csv', purpose: 'analysis'});
  await ledger.revokeExport({id: 'e1', reason: 'source correction requires withdrawal'});
  await assert.rejects(ledger.getExport('e1'), code('EXPORT_REVOKED'));
  await assert.rejects(ledger.export({id: 'e1', query: {}, purpose: 'analysis'}), code('EXPORT_CONFLICT'));
  assert.equal((await ledger.revokeExport({id: 'e1', reason: 'repeat'})).committed, false);
  assert.equal((await ledger.verify()).valid, true);
  assert.equal((await ledger.status()).exports, 1);
});

test('inventory comparison separates missing, newer and conflicting revisions', async t => {
  const {ledger} = await setup(t);
  await ledger.ingest(batch([row(), row({time: '2026-11-01T01:00:00Z'}), row({time: '2026-11-01T02:00:00Z'})]));
  const local = await ledger.inventory({});
  assert.equal(local.entries.length, 3);
  assert.equal(local.digest, digest(local.entries));
  const remote = structuredClone(local.entries);
  remote[0].digest = 'f'.repeat(64);
  remote[1].revision = 2;
  remote.pop();
  const compared = await ledger.reconcile({query: {}, remote});
  assert.equal(compared.conflicts.length, 1);
  assert.equal(compared.behind.length, 1);
  assert.equal(compared.missingRemotely.length, 1);
  assert.equal(compared.equal, 0);
  assert.equal(compared.missingLocally.length, 0);
  await assert.rejects(ledger.reconcile({query: {}, remote: [remote[0], remote[0]]}), code('INVENTORY_DUPLICATE'));
});

test('correction impact traces snapshots and alert lineage', async t => {
  const {ledger} = await setup(t);
  await ledger.ingest(batch());
  await ledger.export({id: 'e', query: {}, purpose: 'analysis'});
  await ledger.saveRule({id: 'r', station: 'p1', metric: 'rainfall', widthMs: 3600000,
    statistic: 'sum', operator: 'gt', threshold: 1});
  await ledger.evaluate({from: '2026-11-01T00:00:00Z', until: '2026-11-01T01:00:00Z'});
  const key = (await ledger.query()).rows[0].key;
  const impact = await ledger.impact({keys: [key]});
  assert.equal(impact.exports[0].id, 'e');
  assert.equal(impact.exports[0].affected, 1);
  assert.equal(impact.alerts[0].rule, 'r');
  assert.equal(impact.observations[0].revision, 1);
  await assert.rejects(ledger.impact({keys: ['missing']}), code('OBSERVATION_MISSING'));
});

test('HTTP enforces authentication, content type and typed query input', async t => {
  const {ledger} = await setup(t);
  const server = await listen(ledger, {port: 0, token: 'test-only-token'});
  t.after(() => new Promise(resolve => server.close(resolve)));
  const base = `http://127.0.0.1:${server.address().port}`;
  assert.equal((await fetch(base + '/status')).status, 401);
  const headers = {authorization: 'Bearer test-only-token'};
  const status = await fetch(base + '/status', {headers});
  assert.equal(status.status, 200);
  assert.equal((await status.json()).activeObservations, 0);
  const wrong = await fetch(base + '/batches', {method: 'POST', headers, body: JSON.stringify(batch())});
  assert.equal(wrong.status, 415);
  const valid = await fetch(base + '/batches', {method: 'POST',
    headers: {...headers, 'content-type': 'application/json', 'x-actor': 'gateway'}, body: JSON.stringify(batch())});
  assert.equal(valid.status, 200);
  assert.equal((await valid.json()).accepted, 1);
  const query = await fetch(base + '/observations?limit=1', {headers});
  assert.equal((await query.json()).rows.length, 1);
  assert.equal((await fetch(base + '/observations?includeDeleted=maybe', {headers})).status, 400);
  assert.equal((await fetch(base + '/missing', {headers})).status, 404);
  assert.equal((await fetch(base + '/status', {method: 'DELETE', headers})).status, 405);
  assert.equal((await ledger.audit({operation: 'batch.ingest'}))[0].actor, 'gateway');
});

test('HTTP optimistic concurrency rejects stale writes and malformed bodies', async t => {
  const {ledger} = await setup(t);
  const server = await listen(ledger, {port: 0});
  t.after(() => new Promise(resolve => server.close(resolve)));
  const base = `http://127.0.0.1:${server.address().port}`;
  const headers = {'content-type': 'application/json'};
  const malformed = await fetch(base + '/batches', {method: 'POST', headers, body: '{'});
  assert.equal(malformed.status, 400);
  assert.equal((await malformed.json()).error, 'INVALID_JSON');
  const stale = await fetch(base + '/batches', {method: 'POST', headers: {...headers, 'if-match': '0'}, body: JSON.stringify(batch())});
  assert.equal(stale.status, 409);
  assert.equal((await stale.json()).error, 'STALE_SEQUENCE');
  const actual = await ledger.status();
  const valid = await fetch(base + '/batches', {method: 'POST', headers: {...headers, 'if-match': String(actual.sequence)},
    body: JSON.stringify(batch())});
  assert.equal(valid.status, 200);
  assert.equal((await ledger.query()).total, 1);
});

test('CLI returns real status and rejects unknown or repeated arguments', async t => {
  const {ledger, root} = await setup(t);
  await ledger.ingest(batch());
  const result = await execute(['status', '--root', root]);
  assert.equal(result.activeObservations, 1);
  assert.equal(result.batches, 1);
  assert.ok((await execute(['help'])).commands.includes('ingest'));
  assert.throws(() => argumentsFor(['status', '--root']), code('ARGUMENTS'));
  assert.throws(() => argumentsFor(['status', '--root', 'a', '--root', 'b']), code('ARGUMENTS'));
  assert.throws(() => argumentsFor(['status', '--unknown', 'x']), code('ARGUMENTS'));
  await assert.rejects(execute(['status']), code('ARGUMENTS'));
  await assert.rejects(execute(['unknown', '--root', root]), code('COMMAND'));
});

test('rolling windows include lookback data and exclude the right boundary', async t => {
  const {ledger} = await setup(t);
  await ledger.ingest(batch([row({value: 1}), row({time: '2026-11-01T01:00:00Z', value: 2}),
    row({time: '2026-11-01T02:00:00Z', value: 3})]));
  const report = rollingSummary(await ledger.store.read(), {station: 'p1', metric: 'rainfall',
    from: '2026-11-01T01:00:00Z', until: '2026-11-01T04:00:00Z', widthMs: 7200000, stepMs: 3600000});
  assert.equal(report.positions.length, 3);
  assert.deepEqual(report.positions.map(x => x.value), [1, 3, 5]);
  assert.deepEqual(report.positions.map(x => x.count), [1, 2, 2]);
  assert.equal(report.positions[0].lineage.length, 1);
  assert.equal(report.positions[2].unit, 'mm');
});

test('local-day coverage handles shifted boundaries and partial days', async t => {
  const {ledger} = await setup(t);
  await ledger.ingest(batch([row(), row({time: '2026-11-01T01:00:00Z', quality: 'estimated'})]));
  const coverage = dailyCoverage(await ledger.store.read(), {station: 'p1', metric: 'rainfall',
    from: '2026-11-01T00:00:00Z', until: '2026-11-01T02:00:00Z', offsetMinutes: 480});
  assert.equal(coverage.days.length, 1);
  assert.equal(coverage.days[0].localDate, '2026-11-01');
  assert.equal(coverage.days[0].expected, 2);
  assert.equal(coverage.days[0].occupied, 2);
  assert.equal(coverage.days[0].coverage, 1);
  assert.equal(coverage.days[0].partialDay, true);
  assert.equal(coverage.days[0].estimated, 1);
});

test('threshold runs break on missing intervals and suspect readings', async t => {
  const {ledger} = await setup(t);
  await ledger.ingest(batch([row({value: 20}), row({time: '2026-11-01T01:00:00Z', value: 25}),
    row({time: '2026-11-01T03:00:00Z', value: 30}),
    row({time: '2026-11-01T04:00:00Z', value: 35, quality: 'suspect'})]));
  const report = thresholdRuns(await ledger.store.read(), {station: 'p1', metric: 'rainfall',
    from: '2026-11-01T00:00:00Z', until: '2026-11-01T05:00:00Z', threshold: 15, minimumDurationMs: 3600000});
  assert.equal(report.runs.length, 1);
  assert.equal(report.runs[0].count, 2);
  assert.equal(report.runs[0].maximum, 25);
  assert.equal(report.runs[0].durationMs, 3600000);
  assert.equal(report.runs[0].keys.length, 2);
});
