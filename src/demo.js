import {mkdtemp, rm} from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import assert from 'node:assert/strict';
import {Ledger} from './service.js';
const root = await mkdtemp(path.join(os.tmpdir(), 'field-ledger-demo-'));
try {
  const ledger = new Ledger(root);
  await ledger.registerSource({id: 'cooperative', name: 'Synthetic cooperative', region: 'delta', metrics: ['rainfall']});
  await ledger.registerStation({id: 'plot1', name: 'Synthetic plot', region: 'delta', latitude: 10, longitude: 20,
    sources: ['cooperative'], activeFrom: '2026-01-01T00:00:00Z'});
  const batch = {id: 'day1', source: 'cooperative', receivedAt: '2026-11-01T03:00:00Z', records: [
    {station: 'plot1', metric: 'rainfall', time: '2026-11-01T00:00:00Z', revision: 1, value: 12, unit: 'mm'},
    {station: 'plot1', metric: 'rainfall', time: '2026-11-01T01:00:00Z', revision: 1, value: -2, unit: 'mm'},
  ]};
  const first = await ledger.ingest(batch);
  assert.equal(first.accepted, 1);
  assert.equal(first.quarantined, 1);
  assert.equal((await ledger.ingest(batch)).replay, true);
  const quarantined = (await ledger.quarantine())[0];
  await ledger.resolve({id: quarantined.id, action: 'replace', actor: 'operator', reason: 'corrected transcription',
    record: {...batch.records[1], value: 3}});
  await ledger.watermark({source: 'cooperative', time: '2026-11-01T02:00:00Z'});
  const query = {station: 'plot1', metric: 'rainfall', from: '2026-11-01T00:00:00Z', until: '2026-11-01T02:00:00Z', widthMs: 7200000};
  assert.equal((await ledger.windows(query)).windows[0].sum, 15);
  await ledger.export({id: 'snapshot1', purpose: 'synthetic agricultural analysis', query: {station: 'plot1'}});
  await ledger.ingest({id: 'correction', source: 'cooperative', receivedAt: '2026-11-01T04:00:00Z',
    records: [{...batch.records[0], revision: 2, value: 5}]});
  assert.equal((await ledger.windows(query)).windows[0].sum, 8);
  assert.equal((await ledger.getExport('snapshot1')).stale, true);
  await ledger.checkpoint();
  const reopened = new Ledger(root);
  assert.equal((await reopened.verify()).valid, true);
  assert.equal((await reopened.status()).activeObservations, 2);
  process.stdout.write(JSON.stringify({accepted: true, status: await reopened.status(), offline: true}) + String.fromCharCode(10));
} finally {
  await rm(root, {recursive: true, force: true});
}
