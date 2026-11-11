import test from 'node:test';
import assert from 'node:assert/strict';
import {writeFile, mkdir} from 'node:fs/promises';
import path from 'node:path';
import {parseCsv, encodeCsv, decodeObservationCsv, decodeJsonLines, importEnvelope} from '../src/adapters.js';
import {mapRows, cumulativeRainfall, alignIntervals} from '../src/transforms.js';
import {prepareImport, runImport, replayImportPlan, simulateBatches} from '../src/pipeline.js';
import {timestamp, canonical, digest, identifier, jsonParse} from '../src/value.js';
import {normalizeMeasurement} from '../src/catalog.js';
import {setup, row, batch, code} from './helpers.js';
const LF = String.fromCharCode(10);
const CR = String.fromCharCode(13);
const Q = String.fromCharCode(34);

test('CSV supports quoted commas, escaped quotes and embedded newlines', () => {
  const input = ['id,value', `one,${Q}a,b${Q}`, `two,${Q}a${Q}${Q}b${Q}`, `three,${Q}line1`, `line2${Q}`].join(LF);
  const rows = parseCsv(input);
  assert.equal(rows.length, 3);
  assert.equal(rows[0].value, 'a,b');
  assert.equal(rows[1].value, 'a' + Q + 'b');
  assert.equal(rows[2].value, 'line1' + LF + 'line2');
  const encoded = encodeCsv(rows, ['id', 'value']);
  assert.deepEqual(parseCsv(encoded), rows);
});

test('CSV handles BOM and CRLF without inventing trailing rows', () => {
  const content = String.fromCharCode(0xfeff) + ['id,value', 'a,1', 'b,2', ''].join(CR + LF);
  const rows = parseCsv(content);
  assert.deepEqual(rows, [{id: 'a', value: '1'}, {id: 'b', value: '2'}]);
  assert.deepEqual(parseCsv('id,value'), []);
  assert.equal(parseCsv('id,value' + LF + 'a,').length, 1);
  assert.equal(parseCsv('id,value' + LF + 'a,')[0].value, '');
});

test('CSV rejects malformed grammar and resource overrun', () => {
  assert.throws(() => parseCsv('id,id' + LF + 'a,b'), code('CSV_HEADER'));
  assert.throws(() => parseCsv('id,value' + LF + 'a'), code('CSV_WIDTH'));
  assert.throws(() => parseCsv('id,value' + LF + 'a,' + Q + 'open'), code('CSV_QUOTE'));
  assert.throws(() => parseCsv('id,value' + LF + 'a,' + Q + 'x' + Q + 'bad'), code('CSV_QUOTE'));
  assert.throws(() => parseCsv('id,value' + LF + 'a,12345', {maxField: 4}), code('LIMIT'));
  assert.throws(() => parseCsv('id' + LF + 'a' + LF + 'b', {maxRows: 1}), code('LIMIT'));
  assert.throws(() => parseCsv('constructor' + LF + 'x'), code('INVALID_ID'));
  assert.throws(() => parseCsv(''), code('CSV_EMPTY'));
});

test('spreadsheet export escapes formula strings but preserves numeric negatives', () => {
  const encoded = encodeCsv([{name: '=1+1', value: -3}, {name: '@bad', value: 0}], ['name', 'value']);
  const rows = parseCsv(encoded);
  assert.equal(rows[0].name, String.fromCharCode(39) + '=1+1');
  assert.equal(rows[0].value, '-3');
  assert.equal(rows[1].name, String.fromCharCode(39) + '@bad');
  assert.equal(rows[1].value, '0');
});

test('observation CSV types revisions and values and treats deletion separately', () => {
  const header = 'station,metric,time,revision,operation,value,unit';
  const input = header + LF + 'p1,rainfall,2026-11-01T00:00:00Z,1,,2,mm' + LF +
    'p1,rainfall,2026-11-01T01:00:00Z,2,delete,,';
  const rows = decodeObservationCsv(input);
  assert.equal(rows[0].revision, 1);
  assert.equal(rows[0].value, 2);
  assert.equal(Object.hasOwn(rows[1], 'value'), false);
  assert.equal(rows[1].operation, 'delete');
  assert.throws(() => decodeObservationCsv(header + LF + 'p1,rainfall,t,1,,NaN,mm'), code('CSV_NUMBER'));
  assert.throws(() => decodeObservationCsv(header + LF + 'p1,rainfall,t,1,delete,2,mm'), code('DELETE_VALUE'));
});

test('JSONL decoding reports malformed rows and enforces count', () => {
  assert.deepEqual(decodeJsonLines('{}' + LF + LF + JSON.stringify({a: 1})), [{}, {a: 1}]);
  assert.throws(() => decodeJsonLines('{}' + LF + '{'), code('INVALID_JSON'));
  assert.throws(() => decodeJsonLines('{}' + LF + '{}', 1), code('LIMIT'));
  const envelope = importEnvelope(JSON.stringify([row()]), 'json',
    {id: 'file1', source: 's1', receivedAt: '2026-11-01T10:00:00Z'});
  assert.equal(envelope.records.length, 1);
  assert.equal(envelope.mode, 'partial');
  assert.throws(() => importEnvelope('', 'xml', {id: 'x', source: 's1', receivedAt: row().time}), code('FORMAT'));
});

test('mapping converts local time and declared calibration without inventing missing values', () => {
  const mapping = {columns: {time: 'when', value: 'reading'}, station: 'p1', metric: 'temperature',
    unit: 'C', scale: 2, offset: -5, offsetMinutes: 480};
  const result = mapRows([{when: '2026-11-01T08:00:00', reading: '10'},
    {when: '2026-11-01T09:00:00', reading: 'NA'}], mapping);
  assert.equal(result.records.length, 1);
  assert.equal(result.records[0].value, 15);
  assert.equal(result.records[0].time, '2026-11-01T00:00:00.000Z');
  assert.equal(result.skipped.length, 1);
  assert.equal(result.skipped[0].index, 1);
  assert.throws(() => mapRows([{when: '2026-11-01T08:00:00', reading: null}], mapping), code('MAPPING_VALUE'));
});

test('rain counters distinguish baseline, reset, wrap and long-gap ambiguity', () => {
  const rows = [row({value: 90}), row({time: '2026-11-01T01:00:00Z', value: 95}),
    row({time: '2026-11-01T02:00:00Z', value: 3})];
  const rejected = cumulativeRainfall(rows);
  assert.equal(rejected.records.length, 1);
  assert.equal(rejected.records[0].value, 5);
  assert.equal(rejected.gaps[0].reason, 'baseline_only');
  assert.equal(rejected.gaps[1].reason, 'counter_decreased');
  const wrapped = cumulativeRainfall(rows, {resetPolicy: 'wrap', counterMaximum: 100});
  assert.equal(wrapped.records[1].value, 8);
  assert.equal(wrapped.records[1].quality, 'estimated');
  const reset = cumulativeRainfall(rows, {resetPolicy: 'zero'});
  assert.equal(reset.records[1].value, 3);
  const gap = cumulativeRainfall(rows, {maximumIntervalMs: 1000});
  assert.equal(gap.records.length, 0);
  assert.equal(gap.gaps[1].reason, 'interval_exceeded');
  assert.throws(() => cumulativeRainfall(rows, {resetPolicy: 'wrap'}), code('COUNTER'));
});

test('interval alignment rejects ambiguous collisions and records outliers', () => {
  const aligned = alignIntervals([row({time: '2026-11-01T00:00:03Z'}),
    row({time: '2026-11-01T01:10:00Z'})], {intervalMs: 3600000, toleranceMs: 5000});
  assert.equal(aligned.records[0].time, '2026-11-01T00:00:00.000Z');
  assert.equal(aligned.rejected.length, 1);
  assert.equal(aligned.rejected[0].index, 1);
  assert.throws(() => alignIntervals([row(), row({time: '2026-11-01T00:00:01Z'})],
    {intervalMs: 3600000, toleranceMs: 2000}), code('ALIGN_COLLISION'));
});

test('manifest import verifies content checksum before changing ledger', async t => {
  const {root, ledger} = await setup(t);
  const inputRoot = path.join(root, 'input');
  await mkdir(inputRoot);
  const content = JSON.stringify([row()]);
  await writeFile(path.join(inputRoot, 'rows.json'), content);
  const manifest = {id: 'file1', source: 's1', receivedAt: '2026-11-01T10:00:00Z',
    files: [{path: 'rows.json', format: 'json', checksum: digest(content)}]};
  const prepared = await prepareImport(inputRoot, manifest);
  assert.equal(prepared.provenance[0].rows, 1);
  const preview = await runImport(ledger, inputRoot, manifest, {preview: true});
  assert.equal(preview.result.result.accepted, 1);
  assert.equal((await ledger.query()).total, 0);
  const committed = await runImport(ledger, inputRoot, manifest);
  assert.equal(committed.result.accepted, 1);
  await writeFile(path.join(inputRoot, 'rows.json'), JSON.stringify([row({value: 20})]));
  await assert.rejects(runImport(ledger, inputRoot, manifest), code('FILE_CHECKSUM'));
  assert.equal((await ledger.query()).rows[0].value, 10);
});

test('multi-file pipeline maps and aligns before atomic ingest', async t => {
  const {root, ledger} = await setup(t);
  const inputRoot = path.join(root, 'input');
  await mkdir(inputRoot);
  const a = 'when,reading' + LF + '2026-11-01T00:00:02Z,10';
  const b = 'when,reading' + LF + '2026-11-01T01:00:02Z,20';
  await writeFile(path.join(inputRoot, 'a.csv'), a);
  await writeFile(path.join(inputRoot, 'b.csv'), b);
  const manifest = {id: 'files', source: 's1', receivedAt: '2026-11-01T10:00:00Z', files: [
    {path: 'a.csv', format: 'csv', checksum: digest(a)},
    {path: 'b.csv', format: 'csv', checksum: digest(b)}],
    mapping: {columns: {time: 'when', value: 'reading'}, station: 'p1', metric: 'rainfall', unit: 'mm'},
    alignment: {intervalMs: 3600000, toleranceMs: 3000}};
  const result = await runImport(ledger, inputRoot, manifest);
  assert.equal(result.result.accepted, 2);
  assert.equal(result.provenance.length, 2);
  assert.equal((await ledger.query()).rows[1].time, '2026-11-01T01:00:00.000Z');
  const plan = await replayImportPlan(ledger, inputRoot, {imports: [manifest], actor: 'replay'});
  assert.equal(plan.results[0].replay, true);
  assert.equal(plan.completed, 1);
});

test('offline simulation sees sequential revisions without mutating source state', async t => {
  const {ledger} = await setup(t);
  const state = await ledger.store.read();
  const result = simulateBatches(state, [batch(), batch([row({revision: 2, value: 2})], {id: 'b2'})]);
  assert.equal(result.results.length, 2);
  assert.equal(Object.values(result.projected.observations)[0].value, 2);
  assert.equal(Object.keys(state.observations).length, 0);
  assert.equal((await ledger.query()).total, 0);
});

test('canonical JSON and strict scalar validation protect identity boundaries', () => {
  assert.equal(canonical({b: 2, a: 1}), canonical({a: 1, b: 2}));
  assert.equal(digest({a: 1}), digest({a: 1}));
  assert.throws(() => canonical({a: NaN}), code('INVALID_NUMBER'));
  assert.throws(() => identifier('__proto__'), code('INVALID_ID'));
  assert.throws(() => timestamp('2026-02-30T00:00:00Z'), code('INVALID_TIME'));
  assert.throws(() => timestamp('2026-11-01 00:00:00'), code('INVALID_TIME'));
  assert.equal(timestamp('2024-02-29T00:00:00Z'), '2024-02-29T00:00:00.000Z');
  assert.throws(() => jsonParse('{'), code('INVALID_JSON'));
});

test('physical units convert only dimension-compatible inputs', () => {
  assert.equal(normalizeMeasurement('rainfall', 1, 'in').value, 25.4);
  assert.equal(normalizeMeasurement('temperature', 273.15, 'K').value, 0);
  assert.equal(normalizeMeasurement('wind_speed', 36, 'km/h').value, 10);
  assert.equal(normalizeMeasurement('pressure', 100000, 'Pa').value, 1000);
  assert.equal(normalizeMeasurement('humidity', 0.5, 'fraction').value, 50);
  assert.throws(() => normalizeMeasurement('rainfall', 1, 'C'), code('UNIT'));
  assert.throws(() => normalizeMeasurement('humidity', 1.1, 'fraction'), code('RANGE'));
  assert.throws(() => normalizeMeasurement('temperature', Infinity, 'C'), code('INVALID_NUMBER'));
});
