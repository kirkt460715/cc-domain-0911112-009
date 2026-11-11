import {promises as fs} from 'node:fs';
import path from 'node:path';
import {randomUUID} from 'node:crypto';
import {directory, withLock, atomicWrite, readOptional, readJson, syncDirectory} from '../files.js';
import {canonical, clone, digest, jsonParse} from '../value.js';
import {fail} from '../errors.js';
import {emptySchedulerState, applySchedulerEvent} from './state.js';
import {normalizeConfig} from './model.js';

const LF = String.fromCharCode(10);

export class SchedulerStore {
  constructor(root, options = {}) {
    this.root = path.resolve(root);
    this.options = options;
    this.journal = path.join(this.root, 'scheduler-journal.jsonl');
    this.checkpoint = path.join(this.root, 'scheduler-checkpoint.json');
  }
  async initialize() {
    await directory(this.root);
    return this;
  }
  async replay({repairTail = false} = {}) {
    const raw = await readOptional(this.journal);
    const state = emptySchedulerState();
    if (raw === null || raw === '') {
      return {state, transactions: [], repaired: false};
    }
    const completeEnd = raw.lastIndexOf(LF) + 1;
    const tail = raw.slice(completeEnd);
    if (tail && !repairTail) {
      fail('SCHEDULER_TAIL', 'Incomplete scheduler journal tail requires locked recovery',
        {bytes: Buffer.byteLength(tail)}, 409);
    }
    const complete = raw.slice(0, completeEnd);
    const transactions = [];
    for (const line of complete.split(LF)) {
      if (!line) {
        continue;
      }
      const transaction = jsonParse(line, 'scheduler transaction');
      if (!Array.isArray(transaction.events) || !transaction.events.length) {
        fail('SCHEDULER_CORRUPT', 'Scheduler transaction has no events', {}, 500);
      }
      this.applyTransaction(state, transaction);
      transactions.push(transaction);
    }
    if (tail && repairTail) {
      const evidence = path.join(this.root, `scheduler-torn-tail-${randomUUID()}.txt`);
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
  applyTransaction(state, transaction) {
    if (transaction.sequence !== state.sequence + 1 || transaction.previous !== state.head) {
      fail('SCHEDULER_CHAIN', 'Scheduler journal sequence or predecessor mismatch', {}, 500);
    }
    const {hash, ...body} = transaction;
    if (digest(body) !== hash) {
      fail('SCHEDULER_HASH', 'Scheduler transaction checksum mismatch', {}, 500);
    }
    const atMs = Date.parse(transaction.time);
    for (const event of transaction.events) {
      applySchedulerEvent(state, event, atMs);
    }
    state.sequence = transaction.sequence;
    state.head = hash;
    return state;
  }
  async read() {
    await this.initialize();
    return withLock(this.root, async () => (await this.replay({repairTail: true})).state, this.options);
  }
  async append(build) {
    await this.initialize();
    return withLock(this.root, async () => {
      const {state} = await this.replay({repairTail: true});
      const draft = clone(state);
      const returned = await build(draft);
      const decision = Array.isArray(returned) ? {events: returned} : returned;
      if (!decision || !Array.isArray(decision.events)) {
        fail('TRANSACTION', 'Invalid scheduler transaction decision', {}, 500);
      }
      if (!decision.events.length) {
        return {result: decision.result ?? null, sequence: state.sequence, committed: false};
      }      const atMs = Date.now();
      for (const event of decision.events) {
        applySchedulerEvent(state, event, atMs);
      }
      const body = {
        version: 1,
        sequence: state.sequence + 1,
        previous: state.head,
        time: new Date(atMs).toISOString(),
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
      return {result: decision.result ?? null, sequence: transaction.sequence, committed: true, state};
    }, this.options);
  }
  async configure(config) {
    const normalized = normalizeConfig(config);
    return this.append(state => {
      if (state.config) {
        return {events: [], result: state.config};
      }
      return {events: [{type: 'scheduler.configured', data: {config: normalized}}], result: normalized};
    });
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
          fail('SCHEDULER_CHECKPOINT', 'Scheduler checkpoint integrity verification failed', {}, 500);
        }
        const prefix = emptySchedulerState();
        for (const tx of transactions.slice(0, body.state.sequence)) {
          this.applyTransaction(prefix, tx);
        }
        if (digest(prefix) !== digest(body.state)) {
          fail('SCHEDULER_CHECKPOINT', 'Scheduler checkpoint diverges from journal', {}, 500);
        }
      }
      return {valid: true, transactions: transactions.length, head: state.head, checkpointValid};
    }, this.options);
  }
}
