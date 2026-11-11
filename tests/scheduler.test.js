import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtemp, rm, readFile} from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import {
  normalizeTask, normalizeConfig, parseRetryAfter, backoffDelay, hostOf, TERMINAL_STATUSES,
} from '../src/scheduler/model.js';
import {
  emptySchedulerState, applySchedulerEvent, selectNextTasks, describeBlocked, buildView,
} from '../src/scheduler/state.js';
import {SchedulerStore} from '../src/scheduler/store.js';
import {callGateway, classifyResponse, buildRequest, parseGatewayPayload, OUTCOMES} from '../src/scheduler/gateway.js';
import {code} from './helpers.js';

const baseTask = (overrides = {}) => ({
  id: 't1',
  source: 's1',
  station: 'p1',
  kind: 'realtime',
  from: '2026-11-01T00:00:00Z',
  until: '2026-11-01T01:00:00Z',
  priority: 100,
  estimatedCost: 2,
  endpoint: 'http://gateway.local/observations?source=s1',
  credentialGroup: 'g1',
  ...overrides,
});

function enqueueEvent(task, at = '2026-11-01T00:00:00Z') {
  return {type: 'task.enqueued', data: {task, at}};
}
function configuredState(config = {}, now = Date.parse('2026-11-01T00:00:00Z')) {
  const state = emptySchedulerState();
  applySchedulerEvent(state, {type: 'scheduler.configured', data: {config: normalizeConfig(config)}}, now);
  return state;
}
function enqueue(state, task, now = Date.parse('2026-11-01T00:00:00Z')) {
  applySchedulerEvent(state, enqueueEvent(task), now);
}
function lease(state, id, now, attempt = 1) {
  applySchedulerEvent(state, {type: 'task.leased', data: {
    id, owner: 'w1', at: new Date(now).toISOString(),
    expiresAt: new Date(now + 60000).toISOString(),
  }}, now);
}

test('task normalization keeps host, credential group and safe headers', () => {
  const task = normalizeTask(baseTask({query: {source: 's1', limit: 100}, headers: {'x-api-key': 'token'}}));
  assert.equal(task.host, 'gateway.local');
  assert.equal(task.query.limit, '100');
  assert.equal(task.headers['x-api-key'], 'token');
  assert.throws(() => normalizeTask(baseTask({headers: {authorization: 'Bearer x'}})), code('HEADER_RESERVED'));
  assert.throws(() => normalizeTask(baseTask({headers: {'X-Credential-Group': 'x'}})), code('HEADER_RESERVED'));
  assert.throws(() => normalizeTask(baseTask({from: '2026-11-02T00:00:00Z', until: '2026-11-01T00:00:00Z'})),
    code('TIME_WINDOW'));
  assert.throws(() => normalizeTask(baseTask({endpoint: 'ftp://gateway.local/x'})), code('ENDPOINT'));
  assert.equal(hostOf('https://gateway.local:8443/x'), 'gateway.local:8443');
  assert.throws(() => normalizeTask(baseTask({id: 'bad id'})), code('INVALID_ID'));
  assert.throws(() => normalizeTask(baseTask({kind: 'speculative'})), code('INVALID_CHOICE'));
  assert.throws(() => normalizeTask(baseTask({body: 'x'})), code('TASK_BODY'));
  const post = normalizeTask(baseTask({id: 't2', method: 'POST', body: '{"a":1}'}));
  assert.equal(post.body, '{"a":1}');
});

test('config defaults and bounds are validated once', () => {
  const config = normalizeConfig();
  assert.equal(config.maxConcurrency, 8);
  assert.equal(config.maxAttempts, 6);
  assert.equal(config.hostRatePerSecond, null);
  assert.throws(() => normalizeConfig({maxConcurrency: 0}), code('INVALID_INTEGER'));
  assert.throws(() => normalizeConfig({baseBackoffMs: 100, maxBackoffMs: 50}), code('CONFIG'));
});

test('retry-after accepts seconds and http dates; backoff honors both', () => {
  const now = Date.parse('2026-11-01T00:00:00Z');
  assert.equal(parseRetryAfter('2', now), 2000);
  assert.equal(parseRetryAfter('0', now), 0);
  assert.equal(parseRetryAfter(new Date(now + 3000).toUTCString(), now), 3000);
  assert.equal(parseRetryAfter('not-a-date', now), null);
  const config = normalizeConfig({baseBackoffMs: 100, maxBackoffMs: 1000, jitterMs: 0});
  assert.equal(backoffDelay(1, config, null, () => 0), 100);
  assert.equal(backoffDelay(2, config, null, () => 0), 200);
  assert.equal(backoffDelay(1, config, 5000, () => 0), 1000);
});

test('reducer drives the full task lifecycle and fencing rejects stale progress', () => {
  const t0 = Date.parse('2026-11-01T00:00:00Z');
  const state = configuredState({budget: 10});
  enqueue(state, normalizeTask(baseTask()), t0);
  assert.equal(state.budget.reserved, 2);
  lease(state, 't1', t0 + 10);
  assert.equal(state.tasks.t1.status, 'leased');
  assert.equal(state.tasks.t1.fencing, 1);
  assert.throws(() => applySchedulerEvent(state, {type: 'task.completed', data: {
    id: 't1', fencing: 0, batchId: 'b', at: new Date(t0 + 20).toISOString(),
  }}, t0 + 20), code('FENCING'));
  applySchedulerEvent(state, {type: 'task.completed', data: {
    id: 't1', fencing: 1, batchId: 'b1', at: new Date(t0 + 20).toISOString(),
  }}, t0 + 20);
  assert.equal(state.tasks.t1.status, 'completed');
  assert.equal(state.tasks.t1.batchId, 'b1');
  assert.equal(state.budget.reserved, 0);
  assert.equal(state.budget.spent, 2);
  assert.equal(state.counters.completed, 1);
  assert.throws(() => lease(state, 't1', t0 + 30), code('TASK_STATE'));
});

test('permanent failure and retry exhaustion are separate terminal states', () => {
  const t0 = Date.parse('2026-11-01T00:00:00Z');
  const state = configuredState();
  enqueue(state, normalizeTask(baseTask()));
  enqueue(state, normalizeTask(baseTask({id: 't2'})));
  lease(state, 't1', t0);
  applySchedulerEvent(state, {type: 'task.permanentlyFailed', data: {
    id: 't1', fencing: 1, at: new Date(t0).toISOString(),
    error: {kind: 'permanent', status: 400, code: null, message: 'bad window'},
  }}, t0);
  assert.equal(state.tasks.t1.status, 'permanent_failed');
  assert.equal(state.counters.permanentFailed, 1);
  assert.equal(state.budget.reserved, 2);
  lease(state, 't2', t0);  applySchedulerEvent(state, {type: 'task.exhausted', data: {
    id: 't2', fencing: 1, at: new Date(t0).toISOString(),
    error: {kind: 'rate_limited', status: 429, code: null, message: 'quota'},
  }}, t0);
  assert.equal(state.tasks.t2.status, 'exhausted');
  assert.equal(state.counters.exhausted, 1);
  const statuses = Object.values(state.tasks).map(item => item.status);
  assert.deepEqual(new Set(statuses), new Set(['permanent_failed', 'exhausted']));
  assert.ok(statuses.every(status => TERMINAL_STATUSES.includes(status)));
});

test('shared budget blocks over-limit enqueue without mutating state', () => {
  const state = configuredState({budget: 3});
  enqueue(state, normalizeTask(baseTask({estimatedCost: 2})));
  const before = state.sequence;
  assert.throws(() => enqueue(state, normalizeTask(baseTask({id: 't2', estimatedCost: 2}))),
    code('BUDGET_EXCEEDED'));
  assert.equal(state.sequence, before);
  assert.equal(state.tasks.t2, undefined);
  assert.equal(state.budget.reserved, 2);
  enqueue(state, normalizeTask(baseTask({id: 't2', estimatedCost: 1})));
  assert.equal(state.budget.reserved, 3);
});

test('pausing a source revokes leases and late progress cannot advance the task', () => {
  const t0 = Date.parse('2026-11-01T00:00:00Z');
  const state = configuredState();
  enqueue(state, normalizeTask(baseTask()));
  lease(state, 't1', t0);
  const fencing = state.tasks.t1.fencing;
  applySchedulerEvent(state, {type: 'source.paused', data: {
    source: 's1', revoked: [{id: 't1', fencing}], at: new Date(t0 + 1000).toISOString(),
  }}, t0 + 1000);
  assert.equal(state.tasks.t1.status, 'paused');
  assert.equal(state.tasks.t1.fencing, fencing + 1);
  assert.throws(() => applySchedulerEvent(state, {type: 'task.completed', data: {
    id: 't1', fencing, batchId: 'late', at: new Date(t0 + 2000).toISOString(),
  }}, t0 + 2000), code('FENCING'));
  assert.equal(state.tasks.t1.status, 'paused');
  assert.throws(() => enqueue(state, normalizeTask(baseTask({id: 't9'}))), code('SOURCE_PAUSED'));
  applySchedulerEvent(state, {type: 'source.resumed', data: {
    source: 's1', tasks: ['t1'], at: new Date(t0 + 3000).toISOString(),
  }}, t0 + 3000);
  assert.equal(state.tasks.t1.status, 'queued');
});

test('lease takeover fences the old executor out of every completion path', () => {
  const t0 = Date.parse('2026-11-01T00:00:00Z');
  const state = configuredState();
  enqueue(state, normalizeTask(baseTask()));
  lease(state, 't1', t0);
  const oldFencing = state.tasks.t1.fencing;
  applySchedulerEvent(state, {type: 'lease.takenOver', data: {
    id: 't1', fencing: oldFencing, at: new Date(t0 + 1000).toISOString(),
  }}, t0 + 1000);
  assert.equal(state.tasks.t1.status, 'queued');
  assert.equal(state.tasks.t1.fencing, oldFencing + 1);
  for (const type of ['task.completed', 'task.permanentlyFailed', 'task.exhausted', 'task.retryScheduled']) {
    assert.throws(() => applySchedulerEvent(state, {type, data: {
      id: 't1', fencing: oldFencing, at: new Date(t0 + 2000).toISOString(),
      error: {kind: 'x', status: null, code: null, message: 'late'},
      notBefore: new Date(t0 + 2000).toISOString(), batchId: 'b',
    }}, t0 + 2000), code('FENCING'));
  }
  assert.equal(state.tasks.t1.status, 'queued');
});

test('selector shares one quota across realtime and backfill and applies host and group limits', () => {
  const t0 = Date.parse('2026-11-01T00:00:00Z');
  const state = configuredState({maxConcurrency: 8, hostConcurrency: 8, groupConcurrency: 2, sourceConcurrency: 1});
  const tasks = [
    baseTask({id: 'rt1', kind: 'realtime', source: 's1', credentialGroup: 'g1', endpoint: 'http://h1/observations'}),
    baseTask({id: 'bf1', kind: 'backfill', source: 's2', credentialGroup: 'g1', endpoint: 'http://h2/history'}),
    baseTask({id: 'rt2', kind: 'realtime', source: 's3', credentialGroup: 'g1', endpoint: 'http://h3/observations'}),
    baseTask({id: 'bf2', kind: 'backfill', source: 's4', credentialGroup: 'g2', endpoint: 'http://h4/history'}),
  ].map(normalizeTask);
  for (const task of tasks) {
    enqueue(state, task);
  }
  const picked = selectNextTasks(state, t0, 8);
  assert.equal(picked.length, 3);
  assert.ok(picked.includes('bf1'));
  assert.ok(picked.includes('bf2'));
  assert.ok(!picked.includes('rt2'));
  lease(state, 'rt1', t0);
  lease(state, 'bf1', t0);
  const blocked = describeBlocked(state, t0);
  assert.ok(blocked.some(item => item.id === 'rt2' && item.reason === 'group_concurrency'));
  assert.ok(!blocked.some(item => item.id === 'bf2'));
});

test('fairness stops one source in a credential group from monopolizing the group', () => {
  const t0 = Date.parse('2026-11-01T00:00:00Z');
  const state = configuredState({groupConcurrency: 1, sourceConcurrency: 1, maxConcurrency: 2, hostConcurrency: 2});
  for (const id of ['a1', 'a2', 'b1']) {
    enqueue(state, normalizeTask(baseTask({
      id, source: id.startsWith('a') ? 'noisy' : 'quiet', credentialGroup: 'shared',
    })));
  }
  const first = selectNextTasks(state, t0, 1);
  assert.equal(first[0], 'a1');
  lease(state, 'a1', t0);
  applySchedulerEvent(state, {type: 'request.recorded', data: {entry: {
    atMs: t0, task: 'a1', source: 'noisy', host: 'gateway.local', key: 'shared',
    dimension: 'group', credentialGroup: 'shared', status: 200, outcome: 'success', attempt: 1,
  }}}, t0);
  applySchedulerEvent(state, {type: 'task.completed', data: {
    id: 'a1', fencing: state.tasks.a1.fencing, batchId: 'b', at: new Date(t0).toISOString(),
  }}, t0);
  const second = selectNextTasks(state, t0, 1);
  assert.equal(second[0], 'b1');
});

test('higher priority wins, then round-robin fairness, then stable id order', () => {
  const t0 = Date.parse('2026-11-01T00:00:00Z');
  const state = configuredState();
  enqueue(state, normalizeTask(baseTask({id: 'low', source: 'sA', priority: 10})));
  enqueue(state, normalizeTask(baseTask({id: 'high', source: 'sB', priority: 900})));
  assert.deepEqual(selectNextTasks(state, t0, 2), ['high', 'low']);
});

test('rate window counts block only the affected host and group', () => {
  const t0 = Date.parse('2026-11-01T00:00:00Z');
  const state = configuredState({hostRatePerSecond: 1, groupRatePerSecond: 1,
    hostConcurrency: 8, groupConcurrency: 8, sourceConcurrency: 8, maxConcurrency: 16});
  enqueue(state, normalizeTask(baseTask({id: 'h1', endpoint: 'http://h1/x'})));
  enqueue(state, normalizeTask(baseTask({id: 'h2', endpoint: 'http://h2/x', credentialGroup: 'g2'})));
  applySchedulerEvent(state, {type: 'request.recorded', data: {entry: {
    atMs: t0, task: 'x', source: 's1', host: 'h1', key: 'h1', dimension: 'host',
    credentialGroup: 'g1', status: 429, outcome: 'rate_limited', attempt: 1,
  }}}, t0);
  applySchedulerEvent(state, {type: 'request.recorded', data: {entry: {
    atMs: t0, task: 'x', source: 's1', host: 'h1', key: 'g1', dimension: 'group',
    credentialGroup: 'g1', status: 429, outcome: 'rate_limited', attempt: 1,
  }}}, t0);
  assert.deepEqual(selectNextTasks(state, t0, 4), ['h2']);
  assert.deepEqual(selectNextTasks(state, t0 + 1001, 4).sort(), ['h1', 'h2']);
  assert.deepEqual(selectNextTasks(state, t0, 1), ['h2']);
});

test('view exposes queues, per-source counters, budget and blocking reasons', () => {
  const t0 = Date.parse('2026-11-01T00:00:00Z');
  const state = configuredState({budget: 10, maxConcurrency: 1});
  enqueue(state, normalizeTask(baseTask()));
  const view = buildView(state, t0);
  assert.equal(view.byStatus.queued, 1);
  assert.equal(view.budget.limit, 10);
  assert.equal(view.budget.available, 8);
  assert.equal(view.nextTaskIds[0], 't1');
  assert.equal(view.bySource.s1.queued, 1);
  assert.equal(view.byCredentialGroup.g1.tasks, 1);
  assert.equal(view.byHost['gateway.local'].tasks, 1);
});

test('gateway response classification separates 429, retryable and permanent statuses', () => {
  assert.equal(classifyResponse(200), OUTCOMES.SUCCESS);
  assert.equal(classifyResponse(429), OUTCOMES.RATE_LIMITED);
  for (const status of [408, 500, 502, 503, 504]) {
    assert.equal(classifyResponse(status), OUTCOMES.RETRYABLE);
  }
  assert.equal(classifyResponse(400), OUTCOMES.PERMANENT);
  assert.equal(classifyResponse(404), OUTCOMES.PERMANENT);
});

test('gateway client sends credential header, parses retry-after and classifies payloads', async () => {
  const seen = [];
  const fetchImpl = async (url, init) => {
    seen.push({url, init});
    return new Response(JSON.stringify({records: [], complete: true}), {
      status: 200, headers: {'content-type': 'application/json'},
    });
  };
  const task = normalizeTask(baseTask());
  const outcome = await callGateway(task, {timeoutMs: 1000, fetchImpl});
  assert.equal(outcome.outcome, OUTCOMES.SUCCESS);
  assert.equal(seen[0].init.headers['x-credential-group'], 'g1');
  assert.ok(seen[0].url.startsWith('http://gateway.local/observations?'));
  const limited = await callGateway(task, {
    timeoutMs: 1000,
    fetchImpl: async () => new Response('{"error":"quota"}', {status: 429, headers: {'retry-after': '2'}}),
  });
  assert.equal(limited.outcome, OUTCOMES.RATE_LIMITED);
  assert.equal(limited.retryAfterMs, 2000);
  const failed = await callGateway(task, {
    timeoutMs: 1000,
    fetchImpl: async () => {
      throw Object.assign(new Error('socket reset'), {name: 'FetchError'});
    },
  });
  assert.equal(failed.outcome, OUTCOMES.RETRYABLE);
  assert.equal(failed.kind, 'network');
  const request = buildRequest({...task, cursor: 'page-2'});
  assert.ok(request.url.includes('cursor=page-2'));
  assert.deepEqual(parseGatewayPayload({records: [], complete: true}), {records: [], complete: true, cursor: null});
  assert.throws(() => parseGatewayPayload({records: [], complete: false}), code('GATEWAY_PAYLOAD'));
  assert.throws(() => parseGatewayPayload({complete: true}), code('GATEWAY_PAYLOAD'));
});

test('scheduler store replays a chained journal and survives a torn tail', async t => {
  const root = await mkdtemp(path.join(os.tmpdir(), 'scheduler-store-'));
  t.after(() => rm(root, {recursive: true, force: true}));
  const store = new SchedulerStore(root);
  await store.configure(normalizeConfig({budget: 5}));
  await store.append(() => ({events: [enqueueEvent(normalizeTask(baseTask({estimatedCost: 1})))]}));
  const state = await store.read();
  assert.equal(state.sequence, 2);
  assert.equal(state.tasks.t1.status, 'queued');
  const verification = await store.verify();
  assert.equal(verification.valid, true);
  const compacted = await store.compact();
  assert.equal(compacted.sequence, 2);
  await import('node:fs/promises').then(fs => fs.appendFile(store.journal, '{"incomplete":'));
  await assert.rejects(store.replay(), code('SCHEDULER_TAIL'));
  const repaired = await store.read();
  assert.equal(repaired.sequence, 2);
  assert.equal(repaired.tasks.t1.status, 'queued');
  const journal = await readFile(store.journal, 'utf8');
  assert.ok(!journal.includes('incomplete'));
  const evidence = await import('node:fs/promises').then(fs =>
    fs.readdir(root).then(names => names.some(name => name.startsWith('scheduler-torn-tail-'))));
  assert.ok(evidence);
});
