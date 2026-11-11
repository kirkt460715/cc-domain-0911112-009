import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtemp, rm, writeFile} from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import {execute} from '../src/cli.js';
import {makeServer} from '../synthetic_exchange/server.js';
import {readFile} from 'node:fs/promises';
import {fileURLToPath} from 'node:url';

const scenarioPath = fileURLToPath(new URL('../synthetic_exchange/scenario.json', import.meta.url));

async function writeJson(dir, name, value) {
  const file = path.join(dir, name);
  await writeFile(file, JSON.stringify(value), 'utf8');
  return file;
}

test('CLI enqueues a scheduler task and the status view tracks it to completion', async t => {
  const root = await mkdtemp(path.join(os.tmpdir(), 'scheduler-cli-'));
  t.after(() => rm(root, {recursive: true, force: true}));
  const scenario = JSON.parse(await readFile(scenarioPath, 'utf8'));
  const server = makeServer(scenario);
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  t.after(() => new Promise(resolve => server.close(resolve)));
  const base = `http://127.0.0.1:${server.address().port}`;
  await execute(['source', '--root', root, '--input', await writeJson(root, 'source.json', {
    id: 's1', name: 'Gateway source', region: 'r1', metrics: ['rainfall'],
  })]);
  await execute(['station', '--root', root, '--input', await writeJson(root, 'station.json', {
    id: 'p1', name: 'Gateway station', region: 'r1', latitude: 10, longitude: 20,
    sources: ['s1'], activeFrom: '2026-01-01T00:00:00Z',
  })]);
  const taskFile = await writeJson(root, 'task.json', {
    id: 'cli-task', source: 's1', station: 'p1', kind: 'realtime',
    from: '2026-11-01T00:00:00Z', until: '2026-11-02T00:00:00Z',
    priority: 100, estimatedCost: 1,
    endpoint: `${base}/observations`, query: {source: 's1'}, credentialGroup: 'g1',
  });
  const enqueued = await execute(['scheduler-enqueue', '--root', root, '--input', taskFile]);
  assert.equal(enqueued.task.id, 'cli-task');
  assert.equal(enqueued.committed, true);
  const status = await execute(['scheduler-status', '--root', root]);
  assert.equal(status.byStatus.queued, 1);
  assert.equal(status.budget.reserved, 1);
  const verification = await execute(['scheduler-verify', '--root', root]);
  assert.equal(verification.valid, true);
  await assert.rejects(execute(['scheduler-takeover', '--root', root, '--input',
    await writeJson(root, 'missing.json', {id: 'ghost'})]), error => error.code === 'TASK_MISSING');
  await assert.rejects(execute(['scheduler-takeover', '--root', root, '--input',
    await writeJson(root, 'queued.json', {id: 'cli-task'})]), error => error.code === 'TASK_STATE');
});

test('CLI pause and resume validate the source and persist scheduler journal events', async t => {
  const root = await mkdtemp(path.join(os.tmpdir(), 'scheduler-cli-pause-'));
  t.after(() => rm(root, {recursive: true, force: true}));
  const pauseInput = await writeJson(root, 'pause.json', {source: 's9'});
  const paused = await execute(['scheduler-pause', '--root', root, '--input', pauseInput]);
  assert.deepEqual(paused.revoked, []);
  const status = await execute(['scheduler-status', '--root', root]);
  assert.equal(status.sources.s9.paused, true);
  const resumed = await execute(['scheduler-resume', '--root', root, '--input', pauseInput]);
  assert.deepEqual(resumed.resumed, []);
  const after = await execute(['scheduler-status', '--root', root]);
  assert.equal(after.sources.s9.paused, false);
});

test('scheduler configuration is immutable once the journal has been configured', async t => {
  const root = await mkdtemp(path.join(os.tmpdir(), 'scheduler-cli-config-'));
  t.after(() => rm(root, {recursive: true, force: true}));
  await execute(['source', '--root', root, '--input', await writeJson(root, 'source.json', {
    id: 's1', name: 'Gateway source', region: 'r1', metrics: ['rainfall'],
  })]);
  await execute(['station', '--root', root, '--input', await writeJson(root, 'station.json', {
    id: 'p1', name: 'Gateway station', region: 'r1', latitude: 10, longitude: 20,
    sources: ['s1'], activeFrom: '2026-01-01T00:00:00Z',
  })]);
  const task = {
    id: 't', source: 's1', station: 'p1', from: '2026-11-01T00:00:00Z', until: '2026-11-02T00:00:00Z',
    endpoint: 'http://127.0.0.1:1/observations', credentialGroup: 'g1', estimatedCost: 1,
  };
  const configA = await writeJson(root, 'a.json', {maxConcurrency: 4, budget: 10});
  await execute(['scheduler-enqueue', '--root', root, '--config', configA,
    '--input', await writeJson(root, 'task.json', task)]);
  const first = await execute(['scheduler-status', '--root', root, '--config', configA]);
  assert.equal(first.config.maxConcurrency, 4);
  const configB = await writeJson(root, 'b.json', {maxConcurrency: 8, budget: 10});
  const statusMismatch = await execute(['scheduler-status', '--root', root, '--config', configB]);
  assert.equal(statusMismatch.config.maxConcurrency, 4);
});
