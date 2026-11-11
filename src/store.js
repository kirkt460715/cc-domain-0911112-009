import {promises as fs} from 'node:fs';
import path from 'node:path';
import {randomUUID} from 'node:crypto';
import {directory, withLock, atomicWrite, readOptional, readJson, syncDirectory} from './files.js';
import {emptyState, applyTransaction, projectEvents} from './state.js';
import {canonical, clone, digest, jsonParse} from './value.js';
import {fail} from './errors.js';
const LF = String.fromCharCode(10);

export class Store {
  constructor(root, options = {}) {
    this.root = path.resolve(root);
    this.options = options;
    this.journal = path.join(this.root, 'journal.jsonl');
    this.checkpoint = path.join(this.root, 'checkpoint.json');
  }
  async initialize() {
    await directory(this.root);
    return this;
  }
  async replay({repairTail = false} = {}) {
    const raw = await readOptional(this.journal);
    const state = emptyState();
    if (raw === null || raw === '') {
      return {state, transactions: [], repaired: false};
    }
    const completeEnd = raw.lastIndexOf(LF) + 1;
    const tail = raw.slice(completeEnd);
    if (tail && !repairTail) {
      fail('JOURNAL_TAIL', 'Incomplete journal tail requires locked recovery', {bytes: Buffer.byteLength(tail)}, 409);
    }
    const complete = raw.slice(0, completeEnd);
    const transactions = [];
    for (const line of complete.split(LF)) {
      if (!line) {
        continue;
      }
      const transaction = jsonParse(line, 'journal transaction');
      if (!Array.isArray(transaction.events) || !transaction.events.length) {
        fail('JOURNAL_CORRUPT', 'Transaction has no events', {}, 500);
      }
      applyTransaction(state, transaction);
      transactions.push(transaction);
    }
    if (tail && repairTail) {
      const evidence = path.join(this.root, `torn-tail-${randomUUID()}.txt`);
      await atomicWrite(evidence, tail);
      const handle = await fs.open(this.journal, 'r+');
      try {
        await handle.truncate(Buffer.byteLength(complete));
        await handle.sync();
      } finally {
        await handle.close();
      }
    }
    return {state, transactions, repaired: Boolean(tail)};
  }
  async read() {
    await this.initialize();
    return withLock(this.root, async () => (await this.replay({repairTail: true})).state, this.options);
  }
  async transact(build, {expectedSequence} = {}) {
    await this.initialize();
    return withLock(this.root, async () => {
      const {state} = await this.replay({repairTail: true});
      if (expectedSequence !== undefined && expectedSequence !== state.sequence) {
        fail('STALE_SEQUENCE', 'Store changed since preview', {actual: state.sequence}, 409);
      }
      const decision = await build(clone(state));
      if (!decision || !Array.isArray(decision.events)) {
        fail('TRANSACTION', 'Invalid transaction decision', {}, 500);
      }
      if (!decision.events.length) {
        return {...decision.result, sequence: state.sequence, committed: false};
      }
      projectEvents(state, decision.events);
      const body = {
        version: 1,
        sequence: state.sequence + 1,
        previous: state.head,
        time: new Date().toISOString(),
        events: decision.events,
      };
      const transaction = {...body, hash: digest(body)};
      const handle = await fs.open(this.journal, 'a', 0o600);
      try {
        await handle.writeFile(canonical(transaction) + LF, 'utf8');
        await handle.sync();
      } finally {
        await handle.close();
      }
      await syncDirectory(this.root);
      return {...decision.result, sequence: transaction.sequence, committed: true};
    }, this.options);
  }
  async compact() {
    await this.initialize();
    return withLock(this.root, async () => {
      const {state, transactions} = await this.replay({repairTail: true});
      const body = {state, transactionCount: transactions.length};
      await atomicWrite(this.checkpoint, canonical({...body, checksum: digest(body)}));
      return {sequence: state.sequence, checksum: digest(body), journalRetained: true};
    }, this.options);
  }
  async verify() {
    await this.initialize();
    return withLock(this.root, async () => {
      const {state, transactions} = await this.replay();
      const checkpoint = await readJson(this.checkpoint);
      let checkpointValid = null;
      if (checkpoint) {
        const {checksum, ...body} = checkpoint;
        checkpointValid = digest(body) === checksum;
        if (!checkpointValid || body.state.sequence > state.sequence) {
          fail('CHECKPOINT_CORRUPT', 'Checkpoint integrity verification failed', {}, 500);
        }
        const prefix = emptyState();
        for (const tx of transactions.slice(0, body.state.sequence)) {
          applyTransaction(prefix, tx);
        }
        if (digest(prefix) !== digest(body.state)) {
          fail('CHECKPOINT_CORRUPT', 'Checkpoint diverges from journal', {}, 500);
        }
      }
      return {valid: true, transactions: transactions.length, head: state.head, checkpointValid};
    }, this.options);
  }
  async backup(destination) {
    await this.initialize();
    if (path.resolve(destination) === this.root) {
      fail('BACKUP_PATH', 'Backup must use another directory');
    }
    return withLock(this.root, async () => {
      const {state} = await this.replay();
      await fs.mkdir(destination, {recursive: false, mode: 0o700});
      const raw = await readOptional(this.journal) ?? '';
      await atomicWrite(path.join(destination, 'journal.jsonl'), raw);
      const manifest = {format: 1, sequence: state.sequence, head: state.head, digest: digest(raw)};
      await atomicWrite(path.join(destination, 'backup.json'), canonical(manifest));
      return manifest;
    }, this.options);
  }
}
