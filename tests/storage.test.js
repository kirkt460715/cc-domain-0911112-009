import test from 'node:test';
import assert from 'node:assert/strict';
import {readFile, writeFile, appendFile, readdir, mkdir, symlink, realpath} from 'node:fs/promises';
import path from 'node:path';
import {Ledger} from '../src/service.js';
import {Store} from '../src/store.js';
import {digest, canonical} from '../src/value.js';
import {localFile, recoverDeadLock} from '../src/files.js';
import {setup, batch, row, code} from './helpers.js';
const LF = String.fromCharCode(10);

test('reopening reconstructs all committed state from the journal', async t => {
  const {ledger, root} = await setup(t);
  await ledger.ingest(batch());
  const before = await ledger.status();
  const reopened = new Ledger(root);
  assert.deepEqual(await reopened.status(), before);
  assert.equal((await reopened.query()).rows[0].value, 10);
  const verification = await reopened.verify();
  assert.equal(verification.valid, true);
  assert.equal(verification.storage.transactions, 3);
  assert.equal(verification.storage.head, before.head);
  const lines = (await readFile(path.join(root, 'journal.jsonl'), 'utf8')).trim().split(LF);
  assert.equal(lines.length, 3);
  assert.equal(JSON.parse(lines[2]).previous, JSON.parse(lines[1]).hash);
});

test('parallel writers serialize against fresh state without losing batches', async t => {
  const {ledger, root} = await setup(t);
  const second = new Ledger(root);
  const writes = [];
  for (let index = 0; index < 12; index += 1) {
    const hour = String(index).padStart(2, '0');
    const writer = index % 2 ? ledger : second;
    writes.push(writer.ingest(batch([row({time: `2026-11-01T${hour}:00:00Z`})],
      {id: `b${index}`, receivedAt: '2026-11-02T00:00:00Z'})));
  }
  const results = await Promise.all(writes);
  assert.equal(new Set(results.map(x => x.sequence)).size, 12);
  assert.equal((await ledger.status()).batches, 12);
  assert.equal((await ledger.query()).total, 12);
  assert.equal((await second.verify()).valid, true);
});

test('concurrent duplicate delivery commits one logical batch', async t => {
  const {ledger, root} = await setup(t);
  const second = new Ledger(root);
  const results = await Promise.all([ledger.ingest(batch()), second.ingest(batch())]);
  assert.equal(results.filter(x => x.committed).length, 1);
  assert.equal(results.filter(x => x.replay).length, 1);
  assert.equal((await ledger.status()).batches, 1);
  assert.equal((await ledger.status()).revisions, 1);
  assert.equal((await ledger.audit({operation: 'batch.ingest'})).length, 1);
});

test('torn final write is quarantined to evidence and prior commits survive', async t => {
  const {ledger, root} = await setup(t);
  await ledger.ingest(batch());
  const status = await ledger.status();
  const file = path.join(root, 'journal.jsonl');
  const partial = JSON.stringify({sequence: 4}).slice(0, -1);
  await appendFile(file, partial);
  await assert.rejects(ledger.store.replay(), code('JOURNAL_TAIL'));
  const reopened = new Ledger(root);
  assert.deepEqual(await reopened.status(), status);
  const evidence = (await readdir(root)).filter(name => name.startsWith('torn-tail-'));
  assert.equal(evidence.length, 1);
  assert.equal(await readFile(path.join(root, evidence[0]), 'utf8'), partial);
  assert.equal((await reopened.verify()).valid, true);
  await reopened.ingest(batch([row({revision: 2, value: 7})], {id: 'b2'}));
  assert.equal((await reopened.query()).rows[0].value, 7);
});

test('complete corrupted transaction fails closed without truncating data', async t => {
  const {ledger, root} = await setup(t);
  await ledger.ingest(batch());
  const file = path.join(root, 'journal.jsonl');
  const lines = (await readFile(file, 'utf8')).trim().split(LF);
  const transaction = JSON.parse(lines[2]);
  transaction.events[0].data.value = 999;
  lines[2] = JSON.stringify(transaction);
  const corrupted = lines.join(LF) + LF;
  await writeFile(file, corrupted);
  await assert.rejects(ledger.status(), code('JOURNAL_HASH'));
  assert.equal(await readFile(file, 'utf8'), corrupted);
  assert.equal((await readdir(root)).filter(name => name.startsWith('torn-tail-')).length, 0);
});

test('missing predecessor is detected even when transaction checksum is valid', async t => {
  const {ledger, root} = await setup(t);
  await ledger.ingest(batch());
  const file = path.join(root, 'journal.jsonl');
  const lines = (await readFile(file, 'utf8')).trim().split(LF);
  const transaction = JSON.parse(lines[2]);
  transaction.previous = '0'.repeat(64);
  const {hash, ...body} = transaction;
  lines[2] = canonical({...body, hash: digest(body)});
  await writeFile(file, lines.join(LF) + LF);
  await assert.rejects(ledger.verify(), code('JOURNAL_CHAIN'));
  await assert.rejects(ledger.ingest(batch([row()], {id: 'new'})), code('JOURNAL_CHAIN'));
});

test('checkpoint remains verifiable after later journal transactions', async t => {
  const {ledger, root} = await setup(t);
  await ledger.ingest(batch());
  const checkpoint = await ledger.checkpoint();
  assert.equal(checkpoint.journalRetained, true);
  await ledger.ingest(batch([row({revision: 2, value: 3})], {id: 'b2'}));
  const reopened = new Ledger(root);
  const verified = await reopened.verify();
  assert.equal(verified.storage.checkpointValid, true);
  assert.equal(verified.storage.transactions, 4);
  assert.equal((await reopened.query()).rows[0].revision, 2);
  const raw = JSON.parse(await readFile(path.join(root, 'checkpoint.json'), 'utf8'));
  assert.equal(raw.state.sequence, checkpoint.sequence);
});

test('checkpoint corruption is reported instead of replacing authoritative journal', async t => {
  const {ledger, root} = await setup(t);
  await ledger.ingest(batch());
  await ledger.checkpoint();
  const file = path.join(root, 'checkpoint.json');
  const checkpoint = JSON.parse(await readFile(file, 'utf8'));
  checkpoint.state.sequence = 800;
  await writeFile(file, JSON.stringify(checkpoint));
  await assert.rejects(ledger.verify(), code('CHECKPOINT_CORRUPT'));
  assert.equal((await ledger.query()).rows[0].value, 10);
  assert.equal((await ledger.status()).sequence, 3);
});

test('backup is a consistent standalone store and refuses existing destination', async t => {
  const {ledger, root} = await setup(t);
  await ledger.ingest(batch());
  const destination = path.join(root, 'backup');
  const manifest = await ledger.backup(destination);
  const recovered = new Ledger(destination);
  assert.equal((await recovered.status()).head, manifest.head);
  assert.equal((await recovered.query()).total, 1);
  assert.equal((await recovered.verify()).valid, true);
  const raw = await readFile(path.join(destination, 'journal.jsonl'), 'utf8');
  assert.equal(digest(raw), manifest.digest);
  await assert.rejects(ledger.backup(destination), error => error.code === 'EEXIST');
  assert.equal((await recovered.query()).rows[0].value, 10);
});

test('lock ownership is explicit and a live writer cannot be forcibly recovered', async t => {
  const {root} = await setup(t);
  const lock = path.join(root, 'writer.lock');
  await writeFile(lock, JSON.stringify({pid: process.pid, token: 'live'}));
  const store = new Store(root, {timeout: 25});
  await assert.rejects(store.read(), code('LOCK_BUSY'));
  await assert.rejects(recoverDeadLock(root), code('LOCK_LIVE'));
  assert.equal(JSON.parse(await readFile(lock, 'utf8')).token, 'live');
});

test('unidentifiable lock is preserved for operator inspection', async t => {
  const {root} = await setup(t);
  const file = path.join(root, 'writer.lock');
  await writeFile(file, JSON.stringify({token: 'incomplete'}));
  await assert.rejects(recoverDeadLock(root), code('LOCK_CORRUPT'));
  assert.deepEqual(JSON.parse(await readFile(file, 'utf8')), {token: 'incomplete'});
});

test('local import path rejects traversal and escaping symlink', async t => {
  const {root} = await setup(t);
  const inputs = path.join(root, 'inputs');
  await mkdir(inputs);
  await writeFile(path.join(inputs, 'data.txt'), 'synthetic');
  assert.equal(await localFile(inputs, 'data.txt'), await realpath(path.join(inputs, 'data.txt')));
  await assert.rejects(localFile(inputs, '../journal.jsonl'), code('UNSAFE_PATH'));
  await symlink(path.join(root, 'journal.jsonl'), path.join(inputs, 'outside'));
  await assert.rejects(localFile(inputs, 'outside'), code('UNSAFE_PATH'));
  await assert.rejects(localFile(inputs, path.join(inputs, 'data.txt')), code('UNSAFE_PATH'));
});
