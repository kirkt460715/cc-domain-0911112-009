import {mkdtemp, rm} from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import {Ledger} from '../src/service.js';
import {SchedulerStore} from '../src/scheduler/store.js';
import {SchedulerEngine} from '../src/scheduler/engine.js';
import {makeServer} from './server.js';
import {readFile} from 'node:fs/promises';
import {fileURLToPath} from 'node:url';

const scenarioPath = fileURLToPath(new URL('./scenario.json', import.meta.url));
const scenario = JSON.parse(await readFile(scenarioPath, 'utf8'));
const server = makeServer(scenario);
await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
const base = `http://127.0.0.1:${server.address().port}`;
const root = await mkdtemp(path.join(os.tmpdir(), 'scheduler-drill-'));
try {
  const ledger = new Ledger(root);
  await ledger.registerSource({id: 's1', name: 'Rate limited source', region: 'r1', metrics: ['rainfall']});
  await ledger.registerSource({id: 's2', name: 'Immediate source', region: 'r1', metrics: ['rainfall']});
  await ledger.registerStation({id: 'p1', name: 'Plot one', region: 'r1', latitude: 10, longitude: 20,
    sources: ['s1'], activeFrom: '2026-01-01T00:00:00Z'});
  await ledger.registerStation({id: 'p2', name: 'Plot two', region: 'r1', latitude: 10, longitude: 20,
    sources: ['s2'], activeFrom: '2026-01-01T00:00:00Z'});
  const engine = new SchedulerEngine({
    store: new SchedulerStore(path.join(root, 'scheduler')),
    ledger,
    config: {
      maxConcurrency: 4, hostConcurrency: 2, groupConcurrency: 2, sourceConcurrency: 1,
      maxAttempts: 5, baseBackoffMs: 50, maxBackoffMs: 2000, leaseMs: 30000, tickMs: 10, budget: 100,
    },
  });
  await engine.start();
  const task = (overrides) => ({
    id: overrides.id, source: overrides.source, station: overrides.station,
    kind: overrides.kind ?? 'realtime', from: '2026-11-01T00:00:00Z', until: '2026-11-02T00:00:00Z',
    priority: overrides.priority ?? 100, estimatedCost: 1,
    endpoint: overrides.endpoint, query: overrides.query, credentialGroup: overrides.credentialGroup ?? 'g1',
  });
  await engine.enqueue(task({id: 'rate-limited', source: 's1', station: 'p1',
    endpoint: `${base}/observations`, query: {source: 's1'}}));
  await engine.enqueue(task({id: 'immediate', source: 's2', station: 'p2',
    endpoint: `${base}/observations`, query: {source: 's2'}}));
  await engine.enqueue(task({id: 'history-backfill', source: 's1', station: 'p1', kind: 'backfill',
    endpoint: `${base}/history`, query: {}}));
  const deadline = Date.now() + 15000;
  for (;;) {
    const view = await engine.view();
    const settled = ['immediate', 'history-backfill'].every(id =>
      view.tasks.find(item => item.id === id)?.status === 'completed');
    if (settled && view.counters.rateLimited >= 1) {
      break;
    }
    if (Date.now() > deadline) {
      throw new Error('scheduler drill did not reach the expected state');
    }
    await new Promise(resolve => setTimeout(resolve, 50));
  }
  const view = await engine.view();
  await engine.stop();
  const observations = await ledger.query();
  const summary = {
    drill: 'fair-scheduling',
    tasks: Object.fromEntries(view.tasks.map(item => [item.id, item.status])),
    rateLimited: view.counters.rateLimited,
    retried: view.counters.retried,
    observations: observations.total,
    budget: {spent: view.budget.spent, reserved: view.budget.reserved, available: view.budget.available},
    blocked: view.blocked,
  };
  process.stdout.write(JSON.stringify(summary) + String.fromCharCode(10));
} finally {
  server.closeAllConnections();
  await new Promise(resolve => server.close(resolve));
  await rm(root, {recursive: true, force: true});
}
