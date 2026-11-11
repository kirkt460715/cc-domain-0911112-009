import test from 'node:test';
import assert from 'node:assert/strict';
import {appendFile, readFile, writeFile, access} from 'node:fs/promises';
import path from 'node:path';
import {inspectBackup, restoreBackup, compareBackupToStore} from '../src/restore.js';
import {Ledger} from '../src/service.js';
import {setup, row, batch, code} from './helpers.js';

test('restore verifies manifest and creates an independent usable store', async t => {
  const {root, ledger} = await setup(t);
  await ledger.ingest(batch());
  const backup = path.join(root, 'backup');
  await ledger.backup(backup);
  const target = path.join(root, 'restored');
  const result = await restoreBackup(backup, target);
  assert.equal(result.restored, true);
  assert.equal(result.sequence, 3);
  const restored = new Ledger(target);
  assert.equal((await restored.query()).rows[0].value, 10);
  await restored.ingest(batch([row({revision: 2, value: 7})], {id: 'b2'}));
  assert.equal((await restored.query()).rows[0].value, 7);
  assert.equal((await ledger.query()).rows[0].value, 10);
  assert.equal((await restored.verify()).valid, true);
});

test('backup inspection rejects tampered content before creating target', async t => {
  const {root, ledger} = await setup(t);
  const backup = path.join(root, 'backup');
  await ledger.backup(backup);
  await appendFile(path.join(backup, 'journal.jsonl'), 'tampered');
  await assert.rejects(inspectBackup(backup), code('BACKUP_CHECKSUM'));
  const target = path.join(root, 'failed-restore');
  await assert.rejects(restoreBackup(backup, target), code('BACKUP_CHECKSUM'));
  await assert.rejects(access(target), error => error.code === 'ENOENT');
  assert.equal((await ledger.verify()).valid, true);
});

test('restore refuses an existing directory and preserves its data', async t => {
  const {root, ledger} = await setup(t);
  const backup = path.join(root, 'backup');
  await ledger.backup(backup);
  const before = await readFile(path.join(root, 'journal.jsonl'), 'utf8');
  await assert.rejects(restoreBackup(backup, root), error => error.code === 'EEXIST');
  assert.equal(await readFile(path.join(root, 'journal.jsonl'), 'utf8'), before);
  await assert.rejects(restoreBackup(backup, backup), code('RESTORE_PATH'));
  assert.equal((await inspectBackup(backup)).manifest.sequence, 2);
});

test('backup comparison reports added and corrected logical observations', async t => {
  const {root, ledger} = await setup(t);
  await ledger.ingest(batch());
  const backup = path.join(root, 'backup');
  await ledger.backup(backup);
  assert.equal((await compareBackupToStore(backup, root)).equal, true);
  await ledger.ingest(batch([row({revision: 2, value: 3}),
    row({time: '2026-11-01T01:00:00Z', value: 4})], {id: 'b2'}));
  const changes = await compareBackupToStore(backup, root);
  assert.equal(changes.equal, false);
  assert.equal(changes.observationsAdded.length, 1);
  assert.equal(changes.observationsChanged.length, 1);
  assert.equal(changes.observationsChanged[0].beforeRevision, 1);
  assert.equal(changes.observationsChanged[0].afterRevision, 2);
  assert.equal(changes.observationsRemoved.length, 0);
});

test('backup manifest cannot claim a different sequence with valid content digest', async t => {
  const {root, ledger} = await setup(t);
  const backup = path.join(root, 'backup');
  await ledger.backup(backup);
  const file = path.join(backup, 'backup.json');
  const manifest = JSON.parse(await readFile(file, 'utf8'));
  manifest.sequence += 1;
  await writeFile(file, JSON.stringify(manifest));
  await assert.rejects(inspectBackup(backup), code('BACKUP_HEAD'));
  assert.equal((await ledger.status()).sequence, 2);
});

test('journal provenance validates actors and reconstructs earlier state', async t => {
  const {ledger} = await setup(t);
  const {auditTransactions, stateAtSequence, lineageGraph} = await import('../src/provenance.js');
  await ledger.ingest(batch(), 'technician');
  await ledger.export({id: 'e', query: {}, purpose: 'analysis'});
  const {transactions} = await ledger.store.replay();
  const audit = auditTransactions(transactions);
  assert.equal(audit.valid, true);
  assert.equal(audit.actors.technician.transactions, 1);
  assert.equal(Object.keys(stateAtSequence(transactions, 2).observations).length, 0);
  assert.equal(Object.keys(stateAtSequence(transactions, 3).observations).length, 1);
  const state = await ledger.store.read();
  const key = Object.keys(state.observations)[0];
  const graph = lineageGraph(state, key);
  assert.equal(graph.nodes.filter(node => node.type === 'export').length, 1);
  assert.equal(graph.edges.filter(edge => edge.relationship === 'included_in').length, 1);
  const altered = structuredClone(transactions);
  altered[2].events.at(-1).data.eventCount = 999;
  assert.equal(auditTransactions(altered).valid, false);
});
