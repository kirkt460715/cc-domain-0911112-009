import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtemp, rm} from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import {Ledger} from '../src/service.js';
import {SchedulerStore} from '../src/scheduler/store.js';
import {SchedulerEngine} from '../src/scheduler/engine.js';
import {listen} from '../src/http.js';
import {code, row} from './helpers.js';

function manualClock(start = Date.now()) {
  const began = Date.now();
  let offset = 0;
  const clock = () => start + (Date.now() - began) + offset;
  clock.advance = ms => {
    offset += ms;
  };
  return clock;
}

function record(overrides = {}) {
  return row({station: 'p-s1', ...overrides});
}

function success(records, extra = {}) {
  return {status: 200, json: {records, complete: true, ...extra}};
}
function rateLimited() {
  return {status: 429, headers: {'retry-after': '1'}, json: {error: 'quota'}};
}
function permanent() {
  return {status: 400, json: {error: 'invalid_window'}};
}

function latch() {
  let opened = false;
  let waiters = [];
  return {
    open() {
      opened = true;
      const pending = waiters;
      waiters = [];
      for (const resolve of pending) {
        resolve();
      }
    },
    get opened() {
      return opened;
    },
    whenOpen() {
      if (opened) {
        return Promise.resolve();
      }
      return new Promise(resolve => waiters.push(resolve));
    },
  };
}

function fakeFetch(router) {
  const calls = [];
  const fetchImpl = async (url, init) => {
    calls.push({url: String(url), init});
    const parsed = new URL(String(url));
    const source = parsed.searchParams.get('source') ?? parsed.pathname.slice(1);
    const handler = router[source] ?? router[parsed.pathname];
    const reply = await handler(calls.length, parsed, init);
    const headers = {'content-type': 'application/json', ...(reply.headers ?? {})};
    const body = reply.json !== undefined ? JSON.stringify(reply.json) : (reply.body ?? '');
    return new Response(body, {status: reply.status ?? 200, headers});
  };
  return {fetchImpl, calls};
}

async function harness(t, {config = {}, router, sources = ['s1', 's2', 's3'], stations} = {}) {
  const root = await mkdtemp(path.join(os.tmpdir(), 'scheduler-engine-'));
  t.after(async () => {
    for (let attempt = 0; attempt < 10; attempt += 1) {
      try {
        await rm(root, {recursive: true, force: true});
        return;
      } catch (error) {
        if (error.code !== 'ENOTEMPTY' && error.code !== 'EBUSY' && error.code !== 'EPERM') {
          throw error;
        }
        await new Promise(resolve => setTimeout(resolve, 25));
      }
    }
    await rm(root, {recursive: true, force: true});
  });
  const ledger = new Ledger(root);
  for (const id of sources) {
    await ledger.registerSource({id, name: `Source ${id}`, region: 'r1', metrics: ['rainfall']});
  }
  const stationList = stations ?? sources.map(id => ({
    id: `p-${id}`, name: `Station ${id}`, region: 'r1', latitude: 10, longitude: 20,
    sources: [id], activeFrom: '2026-01-01T00:00:00Z',
  }));
  for (const station of stationList) {
    await ledger.registerStation(station);
  }
  const store = new SchedulerStore(path.join(root, 'scheduler'));
  const clock = manualClock();
  const {fetchImpl, calls} = fakeFetch(router ?? {});
  const engine = new SchedulerEngine({
    store, ledger,
    config: {tickMs: 10, leaseMs: 30000, baseBackoffMs: 20, maxBackoffMs: 500, jitterMs: 0, ...config},
    fetchImpl, clock, random: () => 0,
  });
  return {root, ledger, store, engine, clock, calls,
    task: (overrides = {}) => ({
      id: overrides.id ?? `task-${Math.random().toString(36).slice(2, 8)}`,
      source: overrides.source ?? 's1',
      station: overrides.station ?? 'p-s1',
      kind: overrides.kind ?? 'realtime',
      from: '2026-11-01T00:00:00Z',
      until: '2026-11-01T02:00:00Z',
      priority: overrides.priority ?? 100,
      estimatedCost: 1,
      endpoint: overrides.endpoint ?? `http://${overrides.source ?? 's1'}.local/observations`,
      query: {source: overrides.source ?? 's1'},
      credentialGroup: overrides.credentialGroup ?? 'g1',
      ...overrides,
    })};
}

test('repeated 429 on one source cannot starve another realtime source or backfill', async t => {
  const h = await harness(t, {
    config: {maxConcurrency: 4, hostConcurrency: 2, groupConcurrency: 4, sourceConcurrency: 1, leaseMs: 30000},
    router: {
      s1: () => rateLimited(),
      s2: () => success([record({station: 'p-s2'})]),
      history: () => success([record({station: 'p-s3', time: '2026-10-31T00:00:00Z'})]),
    },
  });
  await h.engine.start();
  t.after(() => h.engine.stop());
  await h.engine.enqueue(h.task({id: 'noisy', source: 's1', station: 'p-s1', endpoint: 'http://s1.local/observations'}));
  await h.engine.enqueue(h.task({id: 'fresh', source: 's2', station: 'p-s2', endpoint: 'http://s2.local/observations'}));
  await h.engine.enqueue(h.task({
    id: 'backfill', kind: 'backfill', source: 's3', station: 'p-s3',
    endpoint: 'http://history.local/history', query: {source: 'history'},
  }));
  await waitFor(async () => {
    const view = await h.engine.view();
    return view.tasks.find(item => item.id === 'fresh').status === 'completed' &&
      view.tasks.find(item => item.id === 'backfill').status === 'completed' &&
      view.counters.rateLimited >= 1;
  }, 8000);
  const view = await h.engine.view();
  const statusOf = id => view.tasks.find(item => item.id === id)?.status;
  assert.equal(statusOf('fresh'), 'completed', `fresh=${statusOf('fresh')} backfill=${statusOf('backfill')}`);
  assert.equal(statusOf('backfill'), 'completed', `backfill=${statusOf('backfill')}`);
  assert.equal(['waiting_retry', 'leased'].includes(statusOf('noisy')), true,
    `noisy=${statusOf('noisy')}`);
  assert.ok(view.counters.rateLimited >= 1);
  assert.ok(h.calls.some(call => call.url.includes('source=s2')));
  assert.ok(h.calls.some(call => call.url.includes('source=history')));
  const observations = await h.ledger.query();
  assert.equal(observations.total, 2);
});

test('successful task only completes after the batch is ingested into the ledger', async t => {
  const h = await harness(t, {
    router: {s1: () => success([record()])},
  });
  await h.engine.start();
  t.after(() => h.engine.stop());
  const enqueued = await h.engine.enqueue(h.task({id: 'done'}));
  assert.equal(enqueued.committed, true);
  await h.engine.idle(5000);
  const view = await h.engine.view();
  const item = view.tasks.find(entry => entry.id === 'done');
  assert.equal(item.status, 'completed');
  assert.equal(item.batchId, 'done:p1');
  assert.equal(item.attempts, 1);
  assert.equal(view.budget.spent, 1);
  assert.equal(view.budget.reserved, 0);
  const rows = await h.ledger.query();
  assert.equal(rows.total, 1);
  assert.equal(rows.rows[0].revision, 1);
  const batches = await h.ledger.batches();
  assert.equal(batches[0].id, 'done:p1');
});

test('permanent gateway failures stop immediately while retryable failures keep backing off', async t => {
  let s3Attempts = 0;
  const h = await harness(t, {
    config: {maxAttempts: 3, baseBackoffMs: 20, maxBackoffMs: 100},
    router: {
      s1: () => permanent(),
      s3: () => {
        s3Attempts += 1;
        return {status: 503, json: {error: 'busy'}};
      },
    },
  });
  await h.engine.start();
  t.after(() => h.engine.stop());
  await h.engine.enqueue(h.task({id: 'bad-window', source: 's1', station: 'p-s1'}));
  await h.engine.enqueue(h.task({
    id: 'flaky', source: 's3', station: 'p-s3', endpoint: 'http://s3.local/observations', query: {source: 's3'},
  }));
  await h.engine.idle(5000);
  const view = await h.engine.view();
  const bad = view.tasks.find(item => item.id === 'bad-window');
  const flaky = view.tasks.find(item => item.id === 'flaky');
  assert.equal(bad.status, 'permanent_failed');
  assert.equal(bad.attempts, 1);
  assert.equal(bad.lastError.status, 400);
  assert.equal(flaky.status, 'exhausted');
  assert.equal(flaky.attempts, 3);
  assert.equal(s3Attempts, 3);
  assert.equal(view.counters.permanentFailed, 1);
  assert.equal(view.counters.exhausted, 1);
  assert.equal((await h.ledger.query()).total, 0);
});

test('pausing a source fences the in-flight lease so its late response cannot advance the task', async t => {
  const gate = latch();
  const h = await harness(t, {
    router: {
      s1: () => gate.whenOpen().then(() => success([record()])),
      s2: () => success([record({station: 'p-s2'})]),
    },
  });
  await h.engine.start();
  t.after(() => h.engine.stop());
  await h.engine.enqueue(h.task({id: 'paused-task', source: 's1', station: 'p-s1'}));
  await waitFor(() => h.engine.view().then(view =>
    view.tasks.find(item => item.id === 'paused-task')?.status === 'leased'));
  const paused = await h.engine.pauseSource('s1');
  assert.deepEqual(paused.revoked, ['paused-task']);
  const viewPaused = await h.engine.view();
  assert.equal(viewPaused.tasks.find(item => item.id === 'paused-task').status, 'paused');
  gate.open();
  await waitFor(() => h.engine.view().then(view => view.counters.stale >= 1));
  const afterLate = await h.engine.view();
  assert.equal(afterLate.tasks.find(item => item.id === 'paused-task').status, 'paused');
  assert.equal(afterLate.staleRejections[0].reason, 'lease_generation');
  assert.equal((await h.ledger.query({station: 'p-s1'})).total, 0);
  await h.engine.enqueue(h.task({id: 'parallel', source: 's2', station: 'p-s2'}));
  await h.engine.idle(5000);
  assert.equal((await h.engine.view()).tasks.find(item => item.id === 'parallel').status, 'completed');
  const resumed = await h.engine.resumeSource('s1');
  assert.deepEqual(resumed.resumed, ['paused-task']);
  await h.engine.idle(5000);
  assert.equal((await h.engine.view()).tasks.find(item => item.id === 'paused-task').status, 'completed');
});

test('a taken-over lease cannot be confirmed by the previous executor', async t => {
  const firstGate = latch();
  const secondGate = latch();
  let calls = 0;
  const h = await harness(t, {
    config: {requestTimeoutMs: 5000, leaseMs: 5000},
    router: {
      s1: () => {
        calls += 1;
        const gate = calls === 1 ? firstGate : secondGate;
        return gate.whenOpen().then(() => success([record()]));
      },
    },
  });
  await h.engine.start();
  t.after(() => h.engine.stop());
  await h.engine.enqueue(h.task({id: 'owned', source: 's1', station: 'p-s1'}));
  await waitFor(() => h.engine.view().then(view =>
    view.tasks.find(item => item.id === 'owned')?.status === 'leased'));
  const before = await h.engine.view();
  const oldLease = before.tasks.find(item => item.id === 'owned').lease;
  assert.ok(oldLease);
  await h.engine.takeOver('owned');
  const fenced = await h.engine.view();
  assert.equal(fenced.tasks.find(item => item.id === 'owned').status, 'queued');
  firstGate.open();
  await waitFor(() => h.engine.view().then(view => view.counters.stale >= 1));
  assert.equal((await h.engine.view()).counters.stale, 1);
  await waitFor(() => h.calls.length >= 2);
  const reLeased = await h.engine.view();
  assert.equal(reLeased.tasks.find(item => item.id === 'owned').status, 'leased');
  secondGate.open();
  await h.engine.idle(5000);
  const finalView = await h.engine.view();
  assert.equal(finalView.tasks.find(item => item.id === 'owned').status, 'completed');
  assert.equal(finalView.tasks.find(item => item.id === 'owned').attempts, 2);
});

test('the scheduler resumes queued and retry-waiting work after an engine restart', async t => {
  const h = await harness(t, {
    router: {
      s1: (callIndex) => callIndex <= 2 ? rateLimited() : success([record()]),
      s2: () => success([record({station: 'p-s2'})]),
    },
  });
  await h.engine.start();
  await h.engine.enqueue(h.task({id: 'restart-1', source: 's1', station: 'p-s1'}));
  await h.engine.enqueue(h.task({id: 'restart-2', source: 's2', station: 'p-s2'}));
  await waitFor(() => h.engine.view().then(view =>
    view.tasks.find(item => item.id === 'restart-1')?.status === 'waiting_retry'));
  await h.engine.stop();
  const replacement = new SchedulerEngine({
    store: h.store, ledger: h.ledger,
    config: h.engine.config,
    fetchImpl: h.engine.fetchImpl, clock: h.clock, random: () => 0,
  });
  await replacement.start();
  t.after(() => replacement.stop());
  h.clock.advance(2000);
  await replacement.idle(5000);
  const view = await replacement.view();
  assert.equal(view.tasks.find(item => item.id === 'restart-1').status, 'completed');
  assert.equal(view.tasks.find(item => item.id === 'restart-2').status, 'completed');
  assert.equal((await h.ledger.query()).total, 2);
  assert.equal((await replacement.store.verify()).valid, true);
});

test('an expired lease is reclaimed and the late executor is fenced out', async t => {
  const gate = latch();
  const h = await harness(t, {
    config: {leaseMs: 60000, tickMs: 10, maxAttempts: 10},
    router: {s1: () => gate.whenOpen().then(() => success([record()]))},
  });
  await h.engine.start();
  t.after(() => h.engine.stop());
  await h.engine.enqueue(h.task({id: 'expiring', source: 's1', station: 'p-s1'}));
  await waitFor(() => h.engine.view().then(view =>
    view.tasks.find(item => item.id === 'expiring')?.status === 'leased'));
  const leaseBefore = (await h.engine.view()).tasks.find(item => item.id === 'expiring');
  h.clock.advance(61000);
  const recovery = await h.engine.recoverExpiredLeases();
  assert.deepEqual(recovery.recovered, ['expiring']);
  await waitFor(() => h.engine.view().then(view =>
    view.tasks.find(item => item.id === 'expiring')?.status === 'queued'));
  const leaseAfter = (await h.engine.view()).tasks.find(item => item.id === 'expiring');
  assert.ok(leaseAfter.fencing > leaseBefore.fencing);
  assert.equal(leaseAfter.lease, null);
  gate.open();
  await waitFor(() => h.engine.view().then(view => view.counters.stale >= 1));
  await h.engine.idle(5000);
  const view = await h.engine.view();
  assert.equal(view.tasks.find(item => item.id === 'expiring').status, 'completed');
  assert.ok(view.counters.stale >= 1);
});

test('multi-page gateway responses advance cursors and ingest one batch per page', async t => {
  const h = await harness(t, {
    router: {
      s1: (callIndex, url) => {
        const cursor = url.searchParams.get('cursor');
        if (cursor === null) {
          return {status: 200, json: {records: [record()], complete: false, cursor: 'page-2'}};
        }
        assert.equal(cursor, 'page-2');
        return success([record({time: '2026-11-01T01:00:00Z'})]);
      },
    },
  });
  await h.engine.start();
  t.after(() => h.engine.stop());
  await h.engine.enqueue(h.task({id: 'paged'}));
  await h.engine.idle(5000);
  const view = await h.engine.view();
  const item = view.tasks.find(entry => entry.id === 'paged');
  assert.equal(item.status, 'completed');
  assert.equal(item.pages, 1);
  assert.deepEqual(item.batchId, 'paged:p2');
  const batches = await h.ledger.batches();
  assert.deepEqual(batches.map(batch => batch.id).sort(), ['paged:p1', 'paged:p2']);
  assert.equal((await h.ledger.query()).total, 2);
});

test('HTTP endpoints expose the queue and accept pause, resume, takeover and enqueue', async t => {
  const h = await harness(t, {
    router: {s1: () => success([record()])},
  });
  await h.engine.start();
  t.after(() => h.engine.stop());
  const server = await listen(h.ledger, {port: 0, engine: h.engine});
  t.after(() => new Promise(resolve => server.close(resolve)));
  const base = `http://127.0.0.1:${server.address().port}`;
  const post = (route, body) => fetch(base + route, {
    method: 'POST', headers: {'content-type': 'application/json'}, body: JSON.stringify(body),
  }).then(async response => ({status: response.status, body: await response.json()}));
  const enqueued = await post('/scheduler/tasks', h.task({id: 'http-task'}));
  assert.equal(enqueued.status, 200);
  assert.equal(enqueued.body.task.id, 'http-task');
  await h.engine.idle(5000);
  const status = await (await fetch(base + '/scheduler/status')).json();
  assert.equal(status.byStatus.completed, 1);
  assert.equal(status.budget.spent, 1);
  const paused = await post('/scheduler/sources/pause', {source: 's9'});
  assert.equal(paused.status, 200);
  assert.deepEqual(paused.body.revoked, []);
  const resumed = await post('/scheduler/sources/resume', {source: 's9'});
  assert.equal(resumed.status, 200);
  const bad = await post('/scheduler/tasks/takeover', {id: 'missing'});
  assert.equal(bad.status, 400);
  assert.equal(bad.body.error, 'TASK_MISSING');
});

test('budget is shared between realtime and backfill and rejects the over-limit enqueue', async t => {
  const h = await harness(t, {config: {budget: 3}, router: {s1: () => success([record()])}});
  await h.engine.start();
  t.after(() => h.engine.stop());
  await h.engine.enqueue(h.task({id: 'rt', estimatedCost: 2}));
  await assert.rejects(h.engine.enqueue(h.task({id: 'bf', kind: 'backfill', estimatedCost: 2})),
    code('BUDGET_EXCEEDED'));
  await h.engine.enqueue(h.task({id: 'bf', kind: 'backfill', estimatedCost: 1}));
  const view = await h.engine.view();
  assert.equal(view.budget.limit, 3);
  assert.equal(view.tasks.length, 2);
  await h.engine.idle(5000);
  const settled = await h.engine.view();
  assert.equal(settled.budget.spent, 3);
});

async function waitFor(predicate, timeoutMs = 5000) {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    if (await predicate()) {
      return;
    }
    if (Date.now() > deadline) {
      throw new Error('condition was not reached before timeout');
    }
    await new Promise(resolve => setTimeout(resolve, 15));
  }
}
