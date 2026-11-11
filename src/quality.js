import {filterObservations, queryInput, preferSources} from './query.js';
import {statistics} from './windows.js';
import {exactKeys, integer, number, timestamp, identifier} from './value.js';
import {requireValue} from './errors.js';

export function profile(state, input = {}) {
  const query = queryInput(input);
  const rows = filterObservations(state, query);
  const groups = new Map();
  for (const row of rows) {
    const key = [row.station, row.metric, row.source].join('|');
    if (!groups.has(key)) {
      groups.set(key, []);
    }
    groups.get(key).push(row);
  }
  const output = [];
  for (const group of groups.values()) {
    const first = group[0];
    const values = group.filter(row => row.value !== null).map(row => row.value);
    const gaps = [];
    for (let index = 1; index < group.length; index += 1) {
      gaps.push(Date.parse(group[index].time) - Date.parse(group[index - 1].time));
    }
    const interval = state.stations[first.station].intervalMs;
    output.push({
      station: first.station,
      source: first.source,
      metric: first.metric,
      first: first.time,
      last: group.at(-1).time,
      ...statistics(values),
      measured: group.filter(row => row.quality === 'measured').length,
      estimated: group.filter(row => row.quality === 'estimated').length,
      suspect: group.filter(row => row.quality === 'suspect').length,
      corrected: group.filter(row => row.revision > 1).length,
      late: group.filter(row => row.late).length,
      longestGapMs: gaps.length ? Math.max(...gaps) : null,
      irregularIntervals: gaps.filter(gap => gap !== interval).length,
    });
  }
  return {sequence: state.sequence, groups: output};
}
export function detectSpikes(state, input) {
  exactKeys(input, ['station', 'metric', 'from', 'until', 'maximumChange', 'maximumGapMs'],
    ['station', 'metric', 'from', 'until', 'maximumChange']);
  const maximumChange = number(input.maximumChange, 'maximumChange', 0);
  const maximumGap = integer(input.maximumGapMs ?? 86400000, 'maximumGapMs', 1);
  const query = queryInput({station: input.station, metric: input.metric, from: input.from, until: input.until});
  const rows = preferSources(state, filterObservations(state, query));
  const findings = [];
  for (let index = 1; index < rows.length; index += 1) {
    const previous = rows[index - 1];
    const current = rows[index];
    const gap = Date.parse(current.time) - Date.parse(previous.time);
    if (gap > maximumGap || gap <= 0) {
      continue;
    }
    const change = current.value - previous.value;
    if (Math.abs(change) > maximumChange) {
      findings.push({
        key: current.key,
        previousKey: previous.key,
        from: previous.time,
        until: current.time,
        before: previous.value,
        after: current.value,
        change,
        gapMs: gap,
        revision: current.revision,
      });
    }
  }
  return {sequence: state.sequence, inspected: rows.length, findings};
}
export function flatlines(state, input) {
  exactKeys(input, ['station', 'metric', 'from', 'until', 'minimumCount', 'tolerance'],
    ['station', 'metric', 'from', 'until', 'minimumCount']);
  const minimum = integer(input.minimumCount, 'minimumCount', 2, 10000);
  const tolerance = number(input.tolerance ?? 0, 'tolerance', 0);
  const query = queryInput({station: input.station, metric: input.metric, from: input.from, until: input.until});
  const station = state.stations[query.station];
  requireValue(station, 'STATION_MISSING', 'Unknown station');
  const rows = preferSources(state, filterObservations(state, query));
  const findings = [];
  let run = [];
  function flush() {
    if (run.length >= minimum) {
      findings.push({from: run[0].time, until: run.at(-1).time, count: run.length,
        value: run[0].value, keys: run.map(row => row.key)});
    }
    run = [];
  }
  for (const row of rows) {
    const last = run.at(-1);
    if (last && (Math.abs(row.value - run[0].value) > tolerance ||
      Date.parse(row.time) - Date.parse(last.time) !== station.intervalMs)) {
      flush();
    }
    run.push(row);
  }
  flush();
  return {sequence: state.sequence, findings};
}
export function sourceDisagreement(state, input) {
  exactKeys(input, ['station', 'metric', 'from', 'until', 'tolerance'],
    ['station', 'metric', 'from', 'until', 'tolerance']);
  const tolerance = number(input.tolerance, 'tolerance', 0);
  const query = queryInput({station: input.station, metric: input.metric, from: input.from, until: input.until});
  const rows = filterObservations(state, query);
  const grouped = new Map();
  for (const row of rows) {
    if (!grouped.has(row.time)) {
      grouped.set(row.time, []);
    }
    grouped.get(row.time).push(row);
  }
  const findings = [];
  for (const [time, observations] of grouped) {
    if (observations.length < 2) {
      continue;
    }
    const stats = statistics(observations.map(row => row.value));
    if (stats.max - stats.min > tolerance) {
      const preferred = preferSources(state, observations)[0];
      findings.push({time, spread: stats.max - stats.min, preferred: preferred.source,
        observations: observations.map(row => ({source: row.source, value: row.value, key: row.key}))});
    }
  }
  return {sequence: state.sequence, findings};
}
export function freshness(state, input) {
  exactKeys(input, ['at', 'maximumAgeMs', 'region'], ['at', 'maximumAgeMs']);
  const at = timestamp(input.at);
  const maximumAge = integer(input.maximumAgeMs, 'maximumAgeMs', 0);
  const region = input.region ? identifier(input.region) : null;
  const output = [];
  for (const station of Object.values(state.stations)) {
    if (region && station.region !== region) {
      continue;
    }
    if (station.activeFrom > at || station.activeUntil && station.activeUntil <= at) {
      continue;
    }
    for (const source of station.sources) {
      const rows = filterObservations(state, queryInput({station: station.id, source, until: at}));
      const latest = rows.at(-1);
      const age = latest ? Date.parse(at) - Date.parse(latest.time) : null;
      output.push({station: station.id, source, latest: latest?.time ?? null, ageMs: age,
        stale: age === null || age > maximumAge, watermark: state.watermarks[source] ?? null});
    }
  }
  return {at, sources: output};
}
