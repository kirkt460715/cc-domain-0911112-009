import {exactKeys, identifier, integer, timestamp, choice, digest} from './value.js';
import {requireValue} from './errors.js';
import {metric} from './catalog.js';
import {filterObservations, preferSources, queryInput} from './query.js';

export function windowInput(input) {
  exactKeys(input, ['station', 'metric', 'from', 'until', 'widthMs', 'source', 'selection', 'includeSuspect'],
    ['station', 'metric', 'from', 'until', 'widthMs']);
  const result = {
    station: identifier(input.station, 'station'),
    metric: input.metric,
    from: timestamp(input.from, 'from'),
    until: timestamp(input.until, 'until'),
    widthMs: integer(input.widthMs, 'widthMs', 1000, 604800000),
    source: input.source ? identifier(input.source, 'source') : null,
    selection: choice(input.selection ?? 'preferred', ['all', 'preferred'], 'selection'),
    includeSuspect: input.includeSuspect ?? false,
  };
  metric(result.metric);
  requireValue(typeof result.includeSuspect === 'boolean', 'BOOLEAN', 'includeSuspect must be boolean');
  const duration = Date.parse(result.until) - Date.parse(result.from);
  requireValue(duration > 0, 'PERIOD', 'Window end must follow start');
  requireValue(Math.ceil(duration / result.widthMs) <= 10000, 'LIMIT', 'Too many windows');
  return result;
}
export function stableSum(values) {
  let sum = 0;
  let correction = 0;
  for (const value of values) {
    const adjusted = value - correction;
    const next = sum + adjusted;
    correction = (next - sum) - adjusted;
    sum = next;
  }
  return sum;
}
export function statistics(values) {
  if (!values.length) {
    return {count: 0, sum: null, mean: null, min: null, max: null, variance: null, median: null};
  }
  const sorted = [...values].sort((a, b) => a - b);
  const sum = stableSum(values);
  const mean = sum / values.length;
  const variance = stableSum(values.map(value => (value - mean) ** 2)) / values.length;
  const middle = Math.floor(values.length / 2);
  const median = values.length % 2 ? sorted[middle] : (sorted[middle - 1] + sorted[middle]) / 2;
  return {count: values.length, sum, mean, min: sorted[0], max: sorted.at(-1), variance, median};
}
export function aggregateWindows(state, input) {
  const query = windowInput(input);
  const station = state.stations[query.station];
  requireValue(station, 'STATION_MISSING', 'Unknown station');
  if (query.source) {
    requireValue(station.sources.includes(query.source), 'SOURCE_STATION', 'Source is not assigned to station');
  }
  const filter = {station: query.station, metric: query.metric, from: query.from, until: query.until};
  if (query.source) {
    filter.source = query.source;
  }
  let observations = filterObservations(state, queryInput(filter));
  if (!query.includeSuspect) {
    observations = observations.filter(item => item.quality !== 'suspect');
  }
  if (query.selection === 'preferred') {
    observations = preferSources(state, observations);
  }
  const start = Date.parse(query.from);
  const stop = Date.parse(query.until);
  const buckets = new Map();
  for (const observation of observations) {
    const bucket = Math.floor((Date.parse(observation.time) - start) / query.widthMs);
    if (!buckets.has(bucket)) {
      buckets.set(bucket, []);
    }
    buckets.get(bucket).push(observation);
  }
  const sources = (query.source ? [query.source] : station.sources).filter(id =>
    state.sources[id].metrics.includes(query.metric));
  const watermarks = sources.map(id => state.watermarks[id] ?? null);
  const minimumWatermark = watermarks.length && watermarks.every(Boolean) ? [...watermarks].sort()[0] : null;
  const output = [];
  for (let index = 0; start + index * query.widthMs < stop; index += 1) {
    const begin = start + index * query.widthMs;
    const end = Math.min(begin + query.widthMs, stop);
    const rows = buckets.get(index) ?? [];
    const stats = statistics(rows.map(row => row.value));
    const distinctTimes = new Set(rows.map(row => Math.floor((Date.parse(row.time) - begin) / station.intervalMs))).size;
    const expected = Math.ceil((end - begin) / station.intervalMs);
    const endTime = new Date(end).toISOString();
    const final = Boolean(minimumWatermark && endTime <= minimumWatermark);
    const lineage = rows.map(row => ({key: row.key, revision: row.revision, batch: row.batch}));
    output.push({
      station: query.station,
      metric: query.metric,
      from: new Date(begin).toISOString(),
      until: endTime,
      unit: metric(query.metric).unit,
      ...stats,
      value: metric(query.metric).aggregation === 'sum' ? stats.sum : stats.mean,
      expected,
      observedSlots: distinctTimes,
      completeness: Math.min(1, distinctTimes / expected),
      final,
      lateCorrections: rows.filter(row => row.late).length,
      estimated: rows.filter(row => row.quality === 'estimated').length,
      lineage,
      fingerprint: digest(lineage),
    });
  }
  return {sequence: state.sequence, watermark: minimumWatermark, windows: output};
}
export function gaps(state, input) {
  const query = windowInput(input);
  const station = state.stations[query.station];
  requireValue(station, 'STATION_MISSING', 'Unknown station');
  const filter = {station: query.station, metric: query.metric, from: query.from, until: query.until};
  if (query.source) {
    filter.source = query.source;
  }
  const rows = filterObservations(state, queryInput(filter));
  const start = Date.parse(query.from);
  const end = Date.parse(query.until);
  const slots = Math.ceil((end - start) / station.intervalMs);
  requireValue(slots <= 100000, 'LIMIT', 'Gap request contains too many expected slots');
  const present = new Set(rows.map(row => Math.floor((Date.parse(row.time) - start) / station.intervalMs)));
  const ranges = [];
  let open = null;
  for (let index = 0; index < slots; index += 1) {
    if (!present.has(index) && open === null) {
      open = index;
    }
    if (present.has(index) && open !== null) {
      ranges.push({from: new Date(start + open * station.intervalMs).toISOString(),
        until: new Date(start + index * station.intervalMs).toISOString(), slots: index - open});
      open = null;
    }
  }
  if (open !== null) {
    ranges.push({from: new Date(start + open * station.intervalMs).toISOString(),
      until: query.until, slots: slots - open});
  }
  return {station: query.station, expected: slots, present: present.size, missing: slots - present.size, ranges};
}
export function compareWindows(before, after) {
  const changes = [];
  const previous = new Map(before.windows.map(item => [item.from, item]));
  for (const current of after.windows) {
    const old = previous.get(current.from);
    if (!old || old.fingerprint !== current.fingerprint || old.final !== current.final) {
      changes.push({from: current.from, before: old?.value ?? null, after: current.value,
        oldFingerprint: old?.fingerprint ?? null, newFingerprint: current.fingerprint, final: current.final});
    }
    previous.delete(current.from);
  }
  for (const old of previous.values()) {
    changes.push({from: old.from, before: old.value, after: null, oldFingerprint: old.fingerprint, newFingerprint: null});
  }
  return changes.sort((a, b) => a.from.localeCompare(b.from));
}
