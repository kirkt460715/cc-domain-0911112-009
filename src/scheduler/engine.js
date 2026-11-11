import {randomUUID} from 'node:crypto';
import {LedgerError, fail, requireValue} from '../errors.js';
import {canonical} from '../value.js';
import {normalizeTask, normalizeConfig, backoffDelay} from './model.js';
import {selectNextTasks, buildView} from './state.js';
import {callGateway, parseGatewayPayload, OUTCOMES} from './gateway.js';

function errorEntry(error) {
  return {
    kind: error.kind ?? 'error',
    status: error.status ?? null,
    code: error.code ?? null,
    message: error.message ?? 'scheduler failure',
  };
}

export class SchedulerEngine {
  constructor({store, ledger, config, fetchImpl, clock, random, actor = 'scheduler'} = {}) {
    requireValue(store, 'CONFIG', 'SchedulerStore is required');
    requireValue(ledger, 'CONFIG', 'Ledger is required');
    this.store = store;
    this.ledger = ledger;
    this.actor = actor;
    this.fetchImpl = fetchImpl ?? fetch;
    this.clock = clock ?? (() => Date.now());
    this.random = random ?? Math.random;
    this.configInput = config ?? {};
    this.config = null;
    this.running = false;
    this.stopping = false;
    this.timer = null;
    this.workers = new Map();
    this.ownerId = randomUUID();
    this.lastTickError = null;
    this.idleWaiters = [];
  }

  async start() {
    requireValue(!this.running, 'ENGINE_STATE', 'Scheduler engine is already running');
    await this.store.initialize();
    const existing = await this.store.read();
    if (existing.config) {
      const requested = normalizeConfig(this.configInput);
      requireValue(canonical(requested) === canonical(existing.config),
        'CONFIG_STATE', 'Scheduler was configured differently; reuse the stored configuration');
      this.config = existing.config;
    } else {
      const configured = await this.store.configure(this.configInput);
      this.config = configured.result;
    }
    await this.recoverExpiredLeases();
    this.running = true;
    this.stopping = false;
    this.schedulePump(0);
    return {owner: this.ownerId, config: this.config};
  }

  async stop() {
    if (!this.running) {
      return {stopped: true, drained: this.workers.size === 0};
    }
    this.stopping = true;
    if (this.timer) {
      clearTimeout(this.timer);
      this.timer = null;
    }
    while (this.workers.size) {
      await Promise.allSettled([...this.workers.values()]);
    }
    this.running = false;
    this.stopping = false;
    this.signalIdle();
    return {stopped: true, drained: true};
  }

  schedulePump(delayMs) {
    if (!this.running || this.stopping || this.timer !== null) {
      return;
    }
    this.timer = setTimeout(() => {
      this.timer = null;
      this.pump().catch(error => {
        this.lastTickError = error;
        if (this.running && !this.stopping) {
          this.schedulePump(this.config.tickMs);
        }
      });
    }, delayMs);
  }

  kick(delayMs = 0) {
    if (this.timer !== null) {
      clearTimeout(this.timer);
      this.timer = null;
    }
    this.schedulePump(delayMs);
  }

  async pump() {
    await this.recoverExpiredLeases();
    const state = await this.store.read();
    this.config = state.config ?? this.config;
    const slots = this.config.maxConcurrency - this.workers.size;
    const ids = selectNextTasks(state, this.clock(), Math.max(0, slots));
    for (const id of ids) {
      if (this.workers.has(id) || !this.running || this.stopping) {
        continue;
      }
      this.workers.set(id, null);
      const task = await this.leaseTask(id);
      if (!task) {
        this.workers.delete(id);
        continue;
      }
      const worker = this.runWorker(task, task.fencing)
        .catch(error => {
          this.lastTickError = error;
        })
        .finally(() => {
          this.workers.delete(id);
          this.signalIdle();
          this.schedulePump(0);
        });
      this.workers.set(id, worker);
    }
    if (this.running && !this.stopping) {
      this.schedulePump(this.config.tickMs);
    }
    this.signalIdle();
  }

  async leaseTask(id) {
    const nowMs = this.clock();
    const expiresAt = new Date(nowMs + this.config.leaseMs).toISOString();
    const decision = await this.store.append(draft => {
      const item = draft.tasks[id];
      if (!item || !['queued', 'waiting_retry'].includes(item.status) || draft.sources[item.source]?.paused) {
        return {events: [], result: null};
      }
      return {
        events: [{type: 'task.leased', data: {
          id, owner: this.ownerId, at: new Date(nowMs).toISOString(), expiresAt,
        }}],
        result: true,
      };
    });
    if (!decision.committed) {
      return null;
    }
    return decision.state.tasks[id];
  }

  async leaseCurrent(task, fencing) {
    const state = await this.store.read();
    const current = state.tasks[task.id];
    return Boolean(current && current.status === 'leased' && current.fencing === fencing);
  }

  async runWorker(task, fencing) {
    const started = this.clock();
    const result = await callGateway({...task}, {
      timeoutMs: this.config.requestTimeoutMs,
      fetchImpl: this.fetchImpl,
      now: this.clock,
    });
    if (!(await this.leaseCurrent(task, fencing))) {
      await this.rejectProgress(task, fencing, 'lease_generation');
      return;
    }
    await this.recordRequest(task, result, started);
    if (result.outcome === OUTCOMES.PERMANENT) {
      await this.recordTerminal('task.permanentlyFailed', task, fencing, errorEntry(result));
      return;
    }
    if (result.outcome === OUTCOMES.RATE_LIMITED || result.outcome === OUTCOMES.RETRYABLE) {
      await this.scheduleRetry(task, fencing, errorEntry(result), result.retryAfterMs);
      return;
    }
    let parsed;
    try {
      parsed = parseGatewayPayload(result.payload);
    } catch (error) {
      if (error instanceof LedgerError) {
        await this.recordTerminal('task.permanentlyFailed', task, fencing, errorEntry(error));
        return;
      }
      throw error;
    }
    let batchId = null;
    if (parsed.records.length > 0) {
      if (!(await this.leaseCurrent(task, fencing))) {
        await this.rejectProgress(task, fencing, 'lease_generation');
        return;
      }
      batchId = `${task.id}:p${task.pages + 1}`;
      const batch = {
        id: batchId,
        source: task.source,
        receivedAt: new Date(this.clock()).toISOString(),
        mode: 'partial',
        records: parsed.records,
      };
      try {
        await this.ledger.ingest(batch, this.actor);
      } catch (error) {
        if (error instanceof LedgerError) {
          await this.scheduleRetry(task, fencing, errorEntry(error), null);
          return;
        }
        throw error;
      }
      if (!(await this.leaseCurrent(task, fencing))) {
        await this.rejectProgress(task, fencing, 'lease_generation');
        return;
      }
    }
    if (!parsed.complete) {
      const advanced = await this.appendGuarded(task, fencing, () => [{
        type: 'task.pageAdvanced',
        data: {id: task.id, fencing, cursor: parsed.cursor, at: new Date(this.clock()).toISOString()},
      }]);
      if (advanced) {
        await this.requeueAfterPage(task, fencing);
      }
      return;
    }
    await this.recordCompletion(task, fencing, batchId);
  }

  async recordRequest(task, result, startedMs) {
    const common = {
      atMs: startedMs,
      task: task.id,
      source: task.source,
      host: task.host,
      credentialGroup: task.credentialGroup,
      status: result.status,
      outcome: result.outcome,
      attempt: task.attempts,
    };
    await this.store.append(() => ({events: [{
      type: 'request.recorded',
      data: {entry: {...common, key: task.host, dimension: 'host'}},
    }, {
      type: 'request.recorded',
      data: {entry: {...common, key: task.credentialGroup, dimension: 'group'}},
    }]}));
  }

  async appendGuarded(task, fencing, buildEvents) {
    const decision = await this.store.append(draft => {
      const current = draft.tasks[task.id];
      if (!current || current.fencing !== fencing || current.status !== 'leased') {
        return {
          events: [{type: 'progress.rejected', data: {
            id: task.id, fencing, reason: 'lease_generation', at: new Date(this.clock()).toISOString(),
          }}],
          result: {accepted: false},
        };
      }
      return {events: buildEvents(draft), result: {accepted: true}};
    });
    return decision.result?.accepted === true;
  }

  async scheduleRetry(task, fencing, error, retryAfterMs) {
    const state = await this.store.read();
    const current = state.tasks[task.id];
    if (!current || current.fencing !== fencing || current.status !== 'leased') {
      await this.rejectProgress(task, fencing, 'lease_generation');
      return;
    }
    if (current.attempts >= this.config.maxAttempts) {
      await this.appendGuarded(task, fencing, () => [{
        type: 'task.exhausted',
        data: {id: task.id, fencing, error, at: new Date(this.clock()).toISOString()},
      }]);
      return;
    }
    const delayMs = backoffDelay(current.attempts, this.config, retryAfterMs, this.random);
    const notBefore = new Date(this.clock() + delayMs).toISOString();
    const accepted = await this.appendGuarded(task, fencing, () => [{
      type: 'task.retryScheduled',
      data: {id: task.id, fencing, error, notBefore, at: new Date(this.clock()).toISOString()},
    }]);
    if (accepted) {
      this.schedulePump(Math.min(delayMs, this.config.tickMs));
    }
  }

  async requeueAfterPage(task, fencing) {
    const nowMs = this.clock();
    const decision = await this.store.append(draft => {
      const current = draft.tasks[task.id];
      if (!current || current.fencing !== fencing || current.status !== 'leased') {
        return {events: [], result: null};
      }
      return {
        events: [{type: 'task.retryScheduled', data: {
          id: task.id,
          fencing,
          error: {kind: 'page', status: null, code: null, message: 'next gateway page'},
          notBefore: new Date(nowMs).toISOString(),
          at: new Date(nowMs).toISOString(),
        }}],
        result: true,
      };
    });
    if (decision.committed) {
      this.schedulePump(0);
    }
  }

  async recordCompletion(task, fencing, batchId) {
    await this.appendGuarded(task, fencing, () => [{
      type: 'task.completed',
      data: {id: task.id, fencing, batchId, at: new Date(this.clock()).toISOString()},
    }]);
  }

  async recordTerminal(type, task, fencing, error) {
    await this.appendGuarded(task, fencing, () => [{
      type: type,
      data: {id: task.id, fencing, error, at: new Date(this.clock()).toISOString()},
    }]);
  }

  async rejectProgress(task, fencing, reason) {
    await this.store.append(() => [{
      type: 'progress.rejected',
      data: {id: task.id, fencing, reason, at: new Date(this.clock()).toISOString()},
    }]);
  }

  async recoverExpiredLeases() {
    const state = await this.store.read();
    if (!state.config) {
      return {recovered: []};
    }
    const nowMs = this.clock();
    const expired = Object.values(state.tasks)
      .filter(item => item.status === 'leased' && item.lease && Date.parse(item.lease.expiresAt) <= nowMs);
    const recovered = [];
    for (const item of expired) {
      const decision = await this.store.append(draft => {
        const current = draft.tasks[item.id];
        if (!current || current.status !== 'leased' || current.fencing !== item.fencing) {
          return {events: [], result: null};
        }
        return {
          events: [{type: 'lease.takenOver', data: {
            id: item.id, fencing: current.fencing, at: new Date(nowMs).toISOString(),
            reason: 'lease_expired', by: this.ownerId,
          }}],
          result: item.id,
        };
      });
      if (decision.committed) {
        recovered.push(item.id);
      }
    }
    return {recovered};
  }

  async enqueue(input) {
    const task = normalizeTask(input);
    const decision = await this.store.append(() => [{
      type: 'task.enqueued',
      data: {task, at: new Date(this.clock()).toISOString()},
    }]);
    this.schedulePump(0);
    return {task, sequence: decision.sequence, committed: decision.committed};
  }

  async pauseSource(source) {
    const state = await this.store.read();
    const revoked = Object.values(state.tasks)
      .filter(item => item.source === source && item.status === 'leased')
      .map(item => ({id: item.id, fencing: item.fencing}));
    const decision = await this.store.append(() => [{
      type: 'source.paused',
      data: {source, revoked, at: new Date(this.clock()).toISOString()},
    }]);
    return {source, revoked: revoked.map(item => item.id), sequence: decision.sequence};
  }

  async resumeSource(source) {
    const state = await this.store.read();
    const ids = Object.values(state.tasks)
      .filter(item => item.source === source && item.status === 'paused')
      .map(item => item.id);
    const decision = await this.store.append(() => [{
      type: 'source.resumed',
      data: {source, tasks: ids, at: new Date(this.clock()).toISOString()},
    }]);
    this.schedulePump(0);
    return {source, resumed: ids, sequence: decision.sequence};
  }

  async takeOver(taskId) {
    const state = await this.store.read();
    const item = state.tasks[taskId];
    requireValue(item, 'TASK_MISSING', 'Task does not exist', {id: taskId});
    requireValue(item.status === 'leased', 'TASK_STATE', 'Only leased tasks can be taken over',
      {id: taskId, status: item.status});
    const decision = await this.store.append(() => [{
      type: 'lease.takenOver',
      data: {
        id: taskId, fencing: item.fencing, at: new Date(this.clock()).toISOString(),
        reason: 'operator_takeover', by: this.ownerId,
      },
    }]);
    this.schedulePump(0);
    return {id: taskId, fencing: item.fencing, sequence: decision.sequence};
  }

  async view() {
    return buildView(await this.store.read(), this.clock());
  }

  async idle(timeoutMs = 10000) {
    const deadline = Date.now() + timeoutMs;
    for (;;) {
      const state = await this.store.read();
      const active = Object.values(state.tasks).some(item =>
        ['queued', 'leased'].includes(item.status) ||
        (item.status === 'waiting_retry' && Date.parse(item.notBefore) <= this.clock()));
      if (!active && this.workers.size === 0) {
        return;
      }
      if (Date.now() > deadline) {
        fail('ENGINE_IDLE', 'Scheduler did not settle in time', {workers: this.workers.size}, 500);
      }
      await new Promise(resolve => {
        this.idleWaiters.push(resolve);
        setTimeout(resolve, 25);
      });
    }
  }

  signalIdle() {
    const waiters = this.idleWaiters.splice(0);
    for (const resolve of waiters) {
      resolve();
    }
  }
}
