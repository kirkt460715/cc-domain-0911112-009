import test from 'node:test';
import assert from 'node:assert/strict';
import {setup, source, station, row, batch, code} from './helpers.js';

test('batch commits accepted records with stable replay and audit', async t => {
  const {ledger} = await setup(t);
  const input = batch();
  const result = await ledger.ingest(input, 'operator');
  assert.equal(result.accepted, 1);
  assert.equal(result.duplicate, 0);
  assert.equal(result.quarantined, 0);
  assert.equal(result.committed, true);
  const replay = await ledger.ingest(input);
  assert.equal(replay.replay, true);
  assert.equal(replay.sequence, result.sequence);
  assert.equal(replay.committed, false);
  const rows = await ledger.query();
  assert.equal(rows.total, 1);
  assert.equal(rows.rows[0].value, 10);
  assert.equal(rows.rows[0].unit, 'mm');
  const audit = await ledger.audit({operation: 'batch.ingest'});
  assert.equal(audit.length, 1);
  assert.equal(audit[0].actor, 'operator');
  assert.equal(audit[0].sequence, result.sequence);
});

test('batch id collision fails without touching prior result', async t => {
  const {ledger} = await setup(t);
  await ledger.ingest(batch());
  const before = await ledger.status();
  await assert.rejects(ledger.ingest(batch([row({value: 12})])), code('BATCH_CONFLICT'));
  const after = await ledger.status();
  assert.deepEqual(after, before);
  assert.equal((await ledger.query()).rows[0].value, 10);
  assert.equal((await ledger.batches()).length, 1);
});

test('partial mode quarantines invalid data and preserves row indexes', async t => {
  const {ledger} = await setup(t);
  const result = await ledger.ingest(batch([
    row(),
    row({time: '2026-11-01T01:00:00Z', value: -1}),
    row({station: 'unknown'}),
    row({time: '2026-11-01T02:00:00Z', value: 2}),
  ]));
  assert.equal(result.accepted, 2);
  assert.equal(result.quarantined, 2);
  assert.deepEqual(result.outcomes.map(x => x.index), [0, 1, 2, 3]);
  const quarantine = await ledger.quarantine();
  assert.equal(quarantine.length, 2);
  assert.equal(quarantine[0].code, 'RANGE');
  assert.equal(quarantine[0].raw.value, -1);
  assert.equal(quarantine[1].code, 'STATION_MISSING');
  assert.equal((await ledger.verify()).valid, true);
});

test('atomic mode leaves no observation, quarantine or batch after error', async t => {
  const {ledger} = await setup(t);
  const before = await ledger.status();
  await assert.rejects(ledger.ingest(batch([row(), row({value: -1})], {mode: 'atomic'})), code('ATOMIC_REJECTED'));
  assert.deepEqual(await ledger.status(), before);
  assert.equal((await ledger.quarantine()).length, 0);
  assert.equal((await ledger.batches()).length, 0);
  assert.equal((await ledger.query()).total, 0);
});

test('same revision and same normalized value is a duplicate', async t => {
  const {ledger} = await setup(t);
  await ledger.ingest(batch([row({metric: 'temperature', value: 32, unit: 'F'})]));
  const result = await ledger.ingest(batch([row({metric: 'temperature', value: 0, unit: 'C'})], {id: 'b2'}));
  assert.equal(result.duplicate, 1);
  assert.equal(result.accepted, 0);
  const observation = (await ledger.query()).rows[0];
  assert.equal(observation.value, 0);
  assert.equal((await ledger.history(observation.key)).history.length, 1);
});

test('conflicting, stale and skipped revisions have distinct quarantine codes', async t => {
  const {ledger} = await setup(t);
  await ledger.ingest(batch());
  await ledger.ingest(batch([row({revision: 2, value: 20})], {id: 'b2'}));
  const result = await ledger.ingest(batch([
    row({revision: 2, value: 30}),
    row({revision: 1, value: 10}),
    row({revision: 4, value: 40}),
  ], {id: 'b3'}));
  assert.deepEqual(result.outcomes.map(x => x.code), ['REVISION_CONFLICT', 'STALE_REVISION', 'REVISION_GAP']);
  assert.equal(result.quarantined, 3);
  const observation = (await ledger.query()).rows[0];
  assert.equal(observation.value, 20);
  assert.equal(observation.revision, 2);
  assert.equal((await ledger.history(observation.key)).history.length, 2);
});

test('ordered revisions within one batch see preceding rows', async t => {
  const {ledger} = await setup(t);
  const result = await ledger.ingest(batch([row(), row({revision: 2, value: 11}), row({revision: 3, value: 12})]));
  assert.equal(result.accepted, 3);
  const observation = (await ledger.query()).rows[0];
  assert.equal(observation.value, 12);
  const history = await ledger.history(observation.key);
  assert.deepEqual(history.history.map(x => x.value), [10, 11, 12]);
  assert.equal((await ledger.status()).revisions, 3);
});

test('deletion tombstone retains history and can be revised back to active', async t => {
  const {ledger} = await setup(t);
  await ledger.ingest(batch());
  const removed = row({revision: 2, operation: 'delete'});
  delete removed.value;
  delete removed.unit;
  assert.equal((await ledger.ingest(batch([removed], {id: 'b2'}))).accepted, 1);
  assert.equal((await ledger.query()).total, 0);
  const tombstone = (await ledger.query({includeDeleted: true})).rows[0];
  assert.equal(tombstone.value, null);
  assert.equal(tombstone.operation, 'delete');
  await ledger.ingest(batch([row({revision: 3, value: 5})], {id: 'b3'}));
  assert.equal((await ledger.query()).rows[0].value, 5);
  assert.equal((await ledger.history(tombstone.key)).history.length, 3);
  assert.equal((await ledger.verify()).valid, true);
});

test('delete missing observation is isolated and does not create tombstone', async t => {
  const {ledger} = await setup(t);
  const removal = {station: 'p1', metric: 'rainfall', time: row().time, revision: 1, operation: 'delete'};
  const result = await ledger.ingest(batch([removal]));
  assert.equal(result.quarantined, 1);
  assert.equal(result.outcomes[0].code, 'DELETE_MISSING');
  assert.equal((await ledger.status()).deletedObservations, 0);
  assert.equal((await ledger.query({includeDeleted: true})).total, 0);
});

test('timezone normalization produces the same logical observation key', async t => {
  const {ledger} = await setup(t);
  await ledger.ingest(batch([row({time: '2026-11-01T08:00:00+08:00'})]));
  const result = await ledger.ingest(batch([row()], {id: 'b2'}));
  assert.equal(result.duplicate, 1);
  assert.equal((await ledger.query()).total, 1);
  assert.equal((await ledger.query()).rows[0].time, '2026-11-01T00:00:00.000Z');
});

test('future observations are quarantined by source-specific tolerance', async t => {
  const {ledger} = await setup(t);
  const result = await ledger.ingest(batch([row({time: '2026-11-01T10:05:00Z'}),
    row({time: '2026-11-01T10:05:01Z'})]));
  assert.equal(result.accepted, 1);
  assert.equal(result.quarantined, 1);
  assert.equal(result.outcomes[1].code, 'FUTURE');
  assert.equal((await ledger.query()).rows[0].time, '2026-11-01T10:05:00.000Z');
});

test('watermark is monotonic and late horizon distinguishes correction from quarantine', async t => {
  const {ledger} = await setup(t);
  await ledger.watermark({source: 's1', time: '2026-11-02T00:00:00Z'});
  const result = await ledger.ingest(batch([row(), row({time: '2026-10-31T23:59:59Z'})]));
  assert.equal(result.accepted, 1);
  assert.equal(result.late, 1);
  assert.equal(result.outcomes[1].code, 'TOO_LATE');
  await assert.rejects(ledger.watermark({source: 's1', time: '2026-11-01T00:00:00Z'}), code('WATERMARK_REGRESSION'));
  const same = await ledger.watermark({source: 's1', time: '2026-11-02T00:00:00Z'});
  assert.equal(same.committed, false);
  assert.equal((await ledger.query()).rows[0].late, true);
});

test('manual correction resolves quarantine with auditable provenance', async t => {
  const {ledger} = await setup(t);
  await ledger.ingest(batch([row({value: -10})]));
  const item = (await ledger.quarantine())[0];
  const result = await ledger.resolve({id: item.id, action: 'replace', record: row({value: 8}),
    actor: 'technician', reason: 'instrument unit corrected'});
  assert.equal(result.status, 'replaced');
  assert.equal((await ledger.quarantine()).length, 0);
  const resolved = (await ledger.quarantine({status: 'replaced'}))[0];
  assert.equal(resolved.resolution.actor, 'technician');
  const observation = (await ledger.query()).rows[0];
  assert.equal(observation.resolvedFrom, item.id);
  assert.equal(observation.batch, 'b1');
  await assert.rejects(ledger.resolve({id: item.id, action: 'discard', actor: 'x', reason: 'already fixed'}), code('QUARANTINE_RESOLVED'));
});

test('discard requires reason and leaves the bad row out of active data', async t => {
  const {ledger} = await setup(t);
  await ledger.ingest(batch([row({value: -5})]));
  const item = (await ledger.quarantine())[0];
  await assert.rejects(ledger.resolve({id: item.id, action: 'discard', actor: 'operator', reason: ''}), code('REASON'));
  await ledger.resolve({id: item.id, action: 'discard', actor: 'operator', reason: 'unrecoverable reading'});
  assert.equal((await ledger.quarantine()).length, 0);
  assert.equal((await ledger.quarantine({status: 'discarded'})).length, 1);
  assert.equal((await ledger.query()).total, 0);
  assert.equal((await ledger.verify()).valid, true);
});

test('disabled source rejects new batches while existing replay stays stable', async t => {
  const {ledger} = await setup(t);
  await ledger.ingest(batch());
  await ledger.updateSource('s1', {enabled: false});
  await assert.rejects(ledger.ingest(batch([row()], {id: 'b2'})), code('SOURCE_DISABLED'));
  assert.equal((await ledger.ingest(batch())).replay, true);
  assert.equal((await ledger.query()).total, 1);
  await ledger.updateSource('s1', {enabled: true});
  assert.equal((await ledger.ingest(batch([row()], {id: 'b2'}))).duplicate, 1);
});

test('catalog prevents cross-region ownership and retroactive station closure', async t => {
  const {ledger} = await setup(t);
  await ledger.registerSource(source({id: 's2', region: 'r2'}));
  await assert.rejects(ledger.registerStation(station({id: 'p2', sources: ['s2']})), code('REGION'));
  await ledger.ingest(batch());
  await assert.rejects(ledger.updateStation('p1', {activeUntil: '2026-10-31T00:00:00Z'}), code('STATION_DATA'));
  await ledger.updateStation('p1', {activeUntil: '2026-11-02T00:00:00Z'});
  const result = await ledger.ingest(batch([row({time: '2026-11-02T00:00:00Z'})], {id: 'b2', receivedAt: '2026-11-03T00:00:00Z'}));
  assert.equal(result.outcomes[0].code, 'STATION_INACTIVE');
  assert.equal((await ledger.catalog()).stations.length, 1);
});

test('preview is read-only and expected sequence rejects an intervening update', async t => {
  const {ledger} = await setup(t);
  const before = await ledger.status();
  const preview = await ledger.preview(batch());
  assert.equal(preview.after.activeObservations, 1);
  assert.equal(preview.before.activeObservations, 0);
  assert.deepEqual(await ledger.status(), before);
  await ledger.updateSource('s1', {priority: 2});
  await assert.rejects(ledger.ingest(batch(), 'operator', {expectedSequence: preview.sequence}), code('STALE_SEQUENCE'));
  assert.equal((await ledger.query()).total, 0);
  const next = await ledger.preview(batch());
  assert.equal((await ledger.ingest(batch(), 'operator', {expectedSequence: next.sequence})).accepted, 1);
});
