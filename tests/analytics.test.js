import test from 'node:test';
import assert from 'node:assert/strict';
import {setup, source, row, batch, windowQuery, code} from './helpers.js';
import {statistics, compareWindows} from '../src/windows.js';
import {operationsReport, regionReport, sourceScorecard} from '../src/reports.js';

test('windows use half-open boundaries and preserve zero distinct from empty', async t => {
  const {ledger} = await setup(t);
  await ledger.ingest(batch([row({value: 0}), row({time: '2026-11-01T01:00:00Z', value: 4}),
    row({time: '2026-11-01T02:00:00Z', value: 8})]));
  const report = await ledger.windows(windowQuery({widthMs: 3600000, until: '2026-11-01T04:00:00Z'}));
  assert.equal(report.windows.length, 4);
  assert.deepEqual(report.windows.map(x => x.sum), [0, 4, 8, null]);
  assert.deepEqual(report.windows.map(x => x.count), [1, 1, 1, 0]);
  assert.deepEqual(report.windows.map(x => x.completeness), [1, 1, 1, 0]);
  assert.equal(report.windows[3].mean, null);
  assert.equal(report.windows[0].unit, 'mm');
});

test('late correction revises final window and produces different lineage', async t => {
  const {ledger} = await setup(t);
  await ledger.ingest(batch());
  await ledger.watermark({source: 's1', time: '2026-11-01T02:00:00Z'});
  const before = await ledger.windows(windowQuery());
  assert.equal(before.windows[0].final, true);
  await ledger.ingest(batch([row({revision: 2, value: 3})], {id: 'b2'}));
  const after = await ledger.windows(windowQuery());
  assert.equal(after.windows[0].sum, 3);
  assert.equal(after.windows[0].lateCorrections, 1);
  assert.equal(after.windows[0].final, true);
  assert.notEqual(after.windows[0].fingerprint, before.windows[0].fingerprint);
  const changes = compareWindows(before, after);
  assert.equal(changes.length, 1);
  assert.equal(changes[0].before, 10);
  assert.equal(changes[0].after, 3);
});

test('source priority and quality select exactly one observation per slot', async t => {
  const {ledger} = await setup(t);
  await ledger.registerSource(source({id: 's2', priority: 1}));
  const state = await ledger.store.read();
  const station = {...state.stations.p1, id: 'p2', sources: ['s1', 's2']};
  await ledger.registerStation(station);
  await ledger.ingest(batch([row({station: 'p2', value: 10})]));
  await ledger.ingest(batch([row({station: 'p2', value: 20})], {id: 'b2', source: 's2'}));
  const preferred = await ledger.query({station: 'p2', selection: 'preferred'});
  assert.equal(preferred.total, 1);
  assert.equal(preferred.rows[0].source, 's2');
  const report = await ledger.windows(windowQuery({station: 'p2'}));
  assert.equal(report.windows[0].sum, 20);
  const all = await ledger.windows(windowQuery({station: 'p2', selection: 'all'}));
  assert.equal(all.windows[0].sum, 30);
  assert.equal(all.windows[0].observedSlots, 1);
  await ledger.ingest(batch([row({station: 'p2', value: 21, revision: 2, quality: 'estimated'})], {id: 'b3', source: 's2'}));
  assert.equal((await ledger.query({station: 'p2', selection: 'preferred'})).rows[0].source, 's1');
});

test('finality waits for every relevant source watermark', async t => {
  const {ledger} = await setup(t);
  await ledger.registerSource(source({id: 's2'}));
  const state = await ledger.store.read();
  await ledger.registerStation({...state.stations.p1, id: 'p2', sources: ['s1', 's2']});
  await ledger.watermark({source: 's1', time: '2026-11-01T02:00:00Z'});
  assert.equal((await ledger.windows(windowQuery({station: 'p2'}))).windows[0].final, false);
  await ledger.watermark({source: 's2', time: '2026-11-01T01:00:00Z'});
  assert.equal((await ledger.windows(windowQuery({station: 'p2'}))).windows[0].final, false);
  await ledger.watermark({source: 's2', time: '2026-11-01T02:00:00Z'});
  assert.equal((await ledger.windows(windowQuery({station: 'p2'}))).windows[0].final, true);
  assert.equal((await ledger.windows(windowQuery({station: 'p2', source: 's1'}))).watermark, '2026-11-01T02:00:00.000Z');
});

test('suspect rows are excluded by default and temperature aggregates by mean', async t => {
  const {ledger} = await setup(t);
  await ledger.ingest(batch([row({metric: 'temperature', value: 10, unit: 'C'}),
    row({metric: 'temperature', time: '2026-11-01T01:00:00Z', value: 30, unit: 'C', quality: 'suspect'})]));
  const normal = await ledger.windows(windowQuery({metric: 'temperature'}));
  assert.equal(normal.windows[0].value, 10);
  assert.equal(normal.windows[0].count, 1);
  const all = await ledger.windows(windowQuery({metric: 'temperature', includeSuspect: true}));
  assert.equal(all.windows[0].value, 20);
  assert.equal(all.windows[0].sum, 40);
  assert.equal(all.windows[0].variance, 100);
  assert.equal(all.windows[0].median, 20);
});

test('gap ranges merge adjacent missing slots and keep boundary counts', async t => {
  const {ledger} = await setup(t);
  await ledger.ingest(batch([row(), row({time: '2026-11-01T03:00:00Z'})]));
  const gaps = await ledger.gaps(windowQuery({until: '2026-11-01T05:00:00Z'}));
  assert.equal(gaps.expected, 5);
  assert.equal(gaps.present, 2);
  assert.equal(gaps.missing, 3);
  assert.equal(gaps.ranges.length, 2);
  assert.equal(gaps.ranges[0].slots, 2);
  assert.equal(gaps.ranges[0].from, '2026-11-01T01:00:00.000Z');
  assert.equal(gaps.ranges[1].slots, 1);
  assert.equal(gaps.ranges[1].until, '2026-11-01T05:00:00.000Z');
});

test('alert opens, is acknowledged, then retracts after correction', async t => {
  const {ledger} = await setup(t);
  await ledger.ingest(batch());
  await ledger.saveRule({id: 'rain', station: 'p1', metric: 'rainfall', widthMs: 7200000,
    statistic: 'sum', operator: 'gt', threshold: 5});
  const period = {from: '2026-11-01T00:00:00Z', until: '2026-11-01T02:00:00Z'};
  assert.equal((await ledger.evaluate(period)).opened, 1);
  const alert = (await ledger.alerts({active: true}))[0];
  assert.equal(alert.value, 10);
  await ledger.acknowledge({id: alert.id, generation: alert.generation, actor: 'operator', note: 'checked locally'});
  assert.equal((await ledger.alerts())[0].acknowledgement.actor, 'operator');
  assert.equal((await ledger.evaluate(period)).committed, false);
  await ledger.ingest(batch([row({revision: 2, value: 2})], {id: 'b2'}));
  assert.equal((await ledger.evaluate(period)).retracted, 1);
  const retracted = (await ledger.alerts())[0];
  assert.equal(retracted.active, false);
  assert.equal(retracted.generation, 2);
  assert.equal(retracted.reason, 'corrected_below_threshold');
  await assert.rejects(ledger.acknowledge({id: alert.id, generation: 1, actor: 'operator', note: 'stale'}), code('ALERT_CHANGED'));
});

test('final-only alert waits for completeness and finality', async t => {
  const {ledger} = await setup(t);
  await ledger.ingest(batch());
  await ledger.saveRule({id: 'rain', station: 'p1', metric: 'rainfall', widthMs: 7200000,
    statistic: 'sum', operator: 'gte', threshold: 10, minimumCompleteness: 1, finalOnly: true});
  const period = {from: '2026-11-01T00:00:00Z', until: '2026-11-01T02:00:00Z'};
  assert.equal((await ledger.evaluate(period)).opened, 0);
  await ledger.ingest(batch([row({time: '2026-11-01T01:00:00Z', value: 0})], {id: 'b2'}));
  assert.equal((await ledger.evaluate(period)).opened, 0);
  await ledger.watermark({source: 's1', time: period.until});
  assert.equal((await ledger.evaluate(period)).opened, 1);
  await ledger.disableRule('rain');
  assert.equal((await ledger.alerts({active: true})).length, 0);
  assert.equal((await ledger.alerts())[0].reason, 'rule_disabled');
});

test('rule version changes recompute alert even when data stays the same', async t => {
  const {ledger} = await setup(t);
  await ledger.ingest(batch());
  const rule = {id: 'r', station: 'p1', metric: 'rainfall', widthMs: 7200000, statistic: 'sum', operator: 'gt', threshold: 5};
  await ledger.saveRule(rule);
  const period = {from: '2026-11-01T00:00:00Z', until: '2026-11-01T02:00:00Z'};
  await ledger.evaluate(period);
  await ledger.saveRule({...rule, threshold: 20});
  assert.equal((await ledger.evaluate(period)).retracted, 1);
  const alert = (await ledger.alerts())[0];
  assert.equal(alert.ruleVersion, 2);
  assert.equal(alert.threshold, 20);
  await assert.rejects(ledger.evaluate({...period, from: '2026-11-01T00:01:00Z'}), code('WINDOW_ALIGNMENT'));
});

test('spike and flatline checks flag different sensor failure patterns', async t => {
  const {ledger} = await setup(t);
  await ledger.ingest(batch([row({value: 2}), row({time: '2026-11-01T01:00:00Z', value: 2}),
    row({time: '2026-11-01T02:00:00Z', value: 2}), row({time: '2026-11-01T03:00:00Z', value: 20})]));
  const query = {station: 'p1', metric: 'rainfall', from: '2026-11-01T00:00:00Z', until: '2026-11-01T04:00:00Z'};
  const spikes = await ledger.spikes({...query, maximumChange: 5});
  assert.equal(spikes.findings.length, 1);
  assert.equal(spikes.findings[0].change, 18);
  const flatlines = await ledger.flatlines({...query, minimumCount: 3});
  assert.equal(flatlines.findings.length, 1);
  assert.equal(flatlines.findings[0].count, 3);
  assert.equal(flatlines.findings[0].value, 2);
  const profile = await ledger.profile(query);
  assert.equal(profile.groups[0].count, 4);
  assert.equal(profile.groups[0].irregularIntervals, 0);
});

test('operations and source reports expose actionable counts', async t => {
  const {ledger} = await setup(t);
  await ledger.ingest(batch([row(), row({value: -1, time: '2026-11-01T01:00:00Z'})]));
  const state = await ledger.store.read();
  const report = operationsReport(state, {at: '2026-11-03T00:00:00Z'});
  assert.equal(report.attentionRequired, true);
  assert.equal(report.quarantine.byCode.RANGE, 1);
  assert.equal(report.freshness.sources[0].stale, true);
  const score = sourceScorecard(state, 's1');
  assert.equal(score.submitted, 2);
  assert.equal(score.acceptanceRate, 0.5);
  assert.equal(score.openQuarantine, 1);
  const region = regionReport(state, {region: 'r1', from: '2026-11-01T00:00:00Z', until: '2026-11-02T00:00:00Z'});
  assert.equal(region.observations, 1);
  assert.equal(region.metrics.rainfall.stations, 1);
  assert.equal(region.sources, 1);
});

test('statistics calculate stable population variance and median', () => {
  const result = statistics([1, 2, 3, 4]);
  assert.equal(result.mean, 2.5);
  assert.equal(result.variance, 1.25);
  assert.equal(result.median, 2.5);
  assert.equal(result.sum, 10);
  assert.equal(statistics([1, 4, 9]).median, 4);
  assert.equal(statistics([]).sum, null);
  assert.equal(statistics([0]).min, 0);
  assert.equal(statistics([5]).variance, 0);
});
