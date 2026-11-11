import {queryInput, filterObservations, preferSources} from './query.js';
import {exactKeys, integer, number, timestamp, identifier} from './value.js';
import {requireValue} from './errors.js';
import {metric} from './catalog.js';
import {statistics} from './windows.js';

export function rollingSummary(state, input) {
  exactKeys(input, ['station', 'metric', 'from', 'until', 'widthMs', 'stepMs', 'minimumCount'],
    ['station', 'metric', 'from', 'until', 'widthMs', 'stepMs']);
  const station = identifier(input.station);
  const definition = metric(input.metric);
  requireValue(state.stations[station], 'STATION_MISSING', 'Unknown station');
  const from = timestamp(input.from);
  const until = timestamp(input.until);
  requireValue(from < until, 'PERIOD', 'Rolling report end must follow start');
  const width = integer(input.widthMs, 'widthMs', 1000, 604800000);
  const step = integer(input.stepMs, 'stepMs', 1000, width);
  const minimum = integer(input.minimumCount ?? 1, 'minimumCount', 1, 100000);
  const start = Date.parse(from);
  const stop = Date.parse(until);
  requireValue(Math.ceil((stop - start) / step) <= 10000, 'LIMIT', 'Too many rolling positions');
  const historyFrom = new Date(start - width).toISOString();
  const rows = preferSources(state, filterObservations(state, queryInput({
    station, metric: input.metric, from: historyFrom, until,
  }))).filter(row => row.quality !== 'suspect');
  const results = [];
  let left = 0;
  let right = 0;
  for (let end = start; end < stop; end += step) {
    const begin = end - width;
    while (right < rows.length && Date.parse(rows[right].time) < end) {
      right += 1;
    }
    while (left < right && Date.parse(rows[left].time) < begin) {
      left += 1;
    }
    const selected = rows.slice(left, right);
    const stats = statistics(selected.map(row => row.value));
    results.push({
      from: new Date(begin).toISOString(),
      until: new Date(end).toISOString(),
      count: selected.length,
      value: selected.length >= minimum ?
        definition.aggregation === 'sum' ? stats.sum : stats.mean : null,
      min: stats.min,
      max: stats.max,
      unit: definition.unit,
      sufficient: selected.length >= minimum,
      lineage: selected.map(row => ({key: row.key, revision: row.revision})),
    });
  }
  return {sequence: state.sequence, station, metric: input.metric, positions: results};
}
export function dailyCoverage(state, input) {
  exactKeys(input, ['station', 'metric', 'from', 'until', 'offsetMinutes'],
    ['station', 'metric', 'from', 'until']);
  const station = state.stations[identifier(input.station)];
  requireValue(station, 'STATION_MISSING', 'Unknown station');
  metric(input.metric);
  const from = timestamp(input.from);
  const until = timestamp(input.until);
  requireValue(from < until, 'PERIOD', 'Coverage period is invalid');
  const offset = integer(input.offsetMinutes ?? 0, 'offsetMinutes', -840, 840) * 60000;
  const day = 86400000;
  const begin = Date.parse(from);
  const end = Date.parse(until);
  requireValue(Math.ceil((end - begin) / day) <= 3660, 'LIMIT', 'Coverage period is too long');
  const rows = preferSources(state, filterObservations(state, queryInput({
    station: station.id, metric: input.metric, from, until,
  })));
  const grouped = new Map();
  for (const row of rows) {
    const dayStart = Math.floor((Date.parse(row.time) + offset) / day) * day - offset;
    if (!grouped.has(dayStart)) {
      grouped.set(dayStart, []);
    }
    grouped.get(dayStart).push(row);
  }
  const days = [];
  for (let current = Math.floor((begin + offset) / day) * day - offset; current < end; current += day) {
    const start = Math.max(begin, current, Date.parse(station.activeFrom));
    const finish = Math.min(end, current + day, station.activeUntil ? Date.parse(station.activeUntil) : Infinity);
    if (start >= finish) {
      continue;
    }
    const selected = grouped.get(current) ?? [];
    const occupied = new Set(selected.map(row => Math.floor((Date.parse(row.time) - start) / station.intervalMs)));
    const expected = Math.ceil((finish - start) / station.intervalMs);
    const measured = selected.filter(row => row.quality === 'measured').length;
    days.push({
      localDate: new Date(current + offset).toISOString().slice(0, 10),
      from: new Date(start).toISOString(),
      until: new Date(finish).toISOString(),
      expected,
      occupied: occupied.size,
      coverage: Math.min(1, occupied.size / expected),
      measured,
      estimated: selected.filter(row => row.quality === 'estimated').length,
      suspect: selected.filter(row => row.quality === 'suspect').length,
      partialDay: finish - start !== day,
    });
  }
  return {station: station.id, metric: input.metric, offsetMinutes: offset / 60000, days};
}
export function thresholdRuns(state, input) {
  exactKeys(input, ['station', 'metric', 'from', 'until', 'threshold', 'minimumDurationMs', 'maximumGapMs'],
    ['station', 'metric', 'from', 'until', 'threshold']);
  const threshold = number(input.threshold, 'threshold');
  const query = queryInput({station: input.station, metric: input.metric, from: input.from, until: input.until});
  const station = state.stations[query.station];
  requireValue(station, 'STATION_MISSING', 'Unknown station');
  const maximumGap = integer(input.maximumGapMs ?? station.intervalMs, 'maximumGapMs', 1);
  const minimum = integer(input.minimumDurationMs ?? 0, 'minimumDurationMs', 0);
  const rows = preferSources(state, filterObservations(state, query));
  const runs = [];
  let active = [];
  function finish() {
    if (!active.length) {
      return;
    }
    const duration = Date.parse(active.at(-1).time) - Date.parse(active[0].time);
    if (duration >= minimum) {
      runs.push({
        from: active[0].time,
        until: active.at(-1).time,
        durationMs: duration,
        count: active.length,
        maximum: Math.max(...active.map(row => row.value)),
        minimum: Math.min(...active.map(row => row.value)),
        keys: active.map(row => row.key),
      });
    }
    active = [];
  }
  for (const row of rows) {
    if (row.quality === 'suspect' || row.value < threshold) {
      finish();
      continue;
    }
    if (active.length && Date.parse(row.time) - Date.parse(active.at(-1).time) > maximumGap) {
      finish();
    }
    active.push(row);
  }
  finish();
  return {sequence: state.sequence, threshold, runs};
}
