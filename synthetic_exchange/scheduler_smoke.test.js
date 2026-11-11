import test from 'node:test';
import assert from 'node:assert/strict';
import {execFile} from 'node:child_process';
import {fileURLToPath} from 'node:url';
import path from 'node:path';

const drill = fileURLToPath(new URL('./scheduler_drill.js', import.meta.url));

test('scheduler drill keeps realtime and backfill moving while one source is rate limited', async () => {
  const result = await new Promise((resolve, reject) => {
    execFile(process.execPath, [drill], {timeout: 30000}, (error, stdout) => {
      if (error) {
        reject(error);
      } else {
        resolve(JSON.parse(stdout.trim().split('\n').at(-1)));
      }
    });
  });
  assert.equal(result.tasks.immediate, 'completed');
  assert.equal(result.tasks['history-backfill'], 'completed');
  assert.equal(result.tasks['rate-limited'], 'waiting_retry');
  assert.ok(result.rateLimited >= 1);
  assert.equal(result.observations, 2);
  assert.equal(result.blocked[0].reason, 'retry_wait');
  assert.equal(result.budget.available, 97);
});
