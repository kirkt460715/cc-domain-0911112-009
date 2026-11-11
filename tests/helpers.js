import {mkdtemp, rm} from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import {Ledger} from '../src/service.js';
export async function setup(t, options = {}) {
  const root = await mkdtemp(path.join(os.tmpdir(), 'field-test-'));
  t.after(() => rm(root, {recursive: true, force: true}));
  const ledger = new Ledger(root, options);
  await ledger.registerSource(source());
  await ledger.registerStation(station());
  return {root, ledger};
}
export function source(overrides = {}) {
  return {id: 's1', name: 'Synthetic source', region: 'r1', metrics: ['rainfall', 'temperature'],
    latenessMs: 86400000, ...overrides};
}
export function station(overrides = {}) {
  return {id: 'p1', name: 'Synthetic station', region: 'r1', latitude: 10, longitude: 20,
    sources: ['s1'], activeFrom: '2026-01-01T00:00:00Z', ...overrides};
}
export function row(overrides = {}) {
  return {station: 'p1', metric: 'rainfall', time: '2026-11-01T00:00:00Z', revision: 1,
    value: 10, unit: 'mm', ...overrides};
}
export function batch(records = [row()], overrides = {}) {
  return {id: 'b1', source: 's1', receivedAt: '2026-11-01T10:00:00Z', records, ...overrides};
}
export function windowQuery(overrides = {}) {
  return {station: 'p1', metric: 'rainfall', from: '2026-11-01T00:00:00Z',
    until: '2026-11-01T02:00:00Z', widthMs: 7200000, ...overrides};
}
export function code(expected) {
  return error => error.code === expected;
}
