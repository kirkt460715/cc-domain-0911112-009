import {array, choice, exactKeys, identifier, integer, number, timestamp} from './value.js';
import {requireValue} from './errors.js';
import {metric} from './catalog.js';

export function mappingInput(input) {
  exactKeys(input, ['columns', 'station', 'metric', 'unit', 'revision', 'offsetMinutes', 'scale', 'offset', 'missing'], ['columns']);
  exactKeys(input.columns, ['station', 'metric', 'time', 'value', 'revision', 'quality'], ['time', 'value']);
  for (const column of Object.values(input.columns)) {
    identifier(column, 'column');
  }
  if (input.station) {
    identifier(input.station, 'station');
  }
  if (input.metric) {
    metric(input.metric);
  }
  requireValue(input.station || input.columns.station, 'MAPPING', 'Station field or default is required');
  requireValue(input.metric || input.columns.metric, 'MAPPING', 'Metric field or default is required');
  return {
    columns: {...input.columns},
    station: input.station ?? null,
    metric: input.metric ?? null,
    unit: input.unit ?? null,
    revision: integer(input.revision ?? 1, 'revision', 1),
    offsetMinutes: integer(input.offsetMinutes ?? 0, 'offsetMinutes', -840, 840),
    scale: number(input.scale ?? 1, 'scale'),
    offset: number(input.offset ?? 0, 'offset'),
    missing: array(input.missing ?? ['', 'NA'], 'missing', 20),
  };
}
export function mapRows(rows, input) {
  const mapping = mappingInput(input);
  const result = [];
  const skipped = [];
  for (const [index, row] of array(rows, 'rows').entries()) {
    const raw = row[mapping.columns.value];
    if (mapping.missing.includes(raw)) {
      skipped.push({index, reason: 'configured_missing_value'});
      continue;
    }
    const value = typeof raw === 'number' ? raw : Number(raw);
    number(value, 'mapped value');
    requireValue(raw !== null && raw !== undefined && raw !== '', 'MAPPING_VALUE', 'Missing unconfigured value');
    let time = row[mapping.columns.time];
    requireValue(typeof time === 'string', 'MAPPING_TIME', 'Timestamp column must be text');
    if (!/(Z|[+-][0-9]{2}:[0-9]{2})$/.test(time)) {
      const offset = mapping.offsetMinutes;
      const sign = offset < 0 ? '-' : '+';
      const hours = String(Math.floor(Math.abs(offset) / 60)).padStart(2, '0');
      const minutes = String(Math.abs(offset) % 60).padStart(2, '0');
      time += `${sign}${hours}:${minutes}`;
    }
    const name = mapping.columns.metric ? row[mapping.columns.metric] : mapping.metric;
    const unit = mapping.unit ?? metric(name).unit;
    const station = mapping.columns.station ? row[mapping.columns.station] : mapping.station;
    const revision = mapping.columns.revision ? Number(row[mapping.columns.revision]) : mapping.revision;
    result.push({
      station: identifier(station, 'station'),
      metric: name,
      time: timestamp(time),
      revision: integer(revision, 'revision', 1),
      value: number(value * mapping.scale + mapping.offset, 'transformed value'),
      unit,
      quality: mapping.columns.quality ? row[mapping.columns.quality] : 'measured',
    });
  }
  return {records: result, skipped};
}
export function cumulativeRainfall(rows, input = {}) {
  exactKeys(input, ['counterMaximum', 'maximumIntervalMs', 'resetPolicy', 'initial'], []);
  const maximum = input.counterMaximum === undefined ? null : number(input.counterMaximum, 'counterMaximum', 0.001);
  const maximumInterval = integer(input.maximumIntervalMs ?? 86400000, 'maximumIntervalMs', 1);
  const resetPolicy = choice(input.resetPolicy ?? 'reject', ['reject', 'zero', 'wrap'], 'resetPolicy');
  requireValue(resetPolicy !== 'wrap' || maximum !== null, 'COUNTER', 'Wrap mode needs counterMaximum');
  const sorted = [...array(rows, 'rows')].sort((a, b) => a.time.localeCompare(b.time));
  const records = [];
  const gaps = [];
  const previous = new Map();
  for (const row of sorted) {
    const station = identifier(row.station);
    const time = timestamp(row.time);
    const value = number(row.value, 'counter value', 0, maximum ?? Infinity);
    const old = previous.get(station);
    if (!old) {
      if (input.initial !== undefined) {
        const initial = number(input.initial, 'initial', 0);
        requireValue(value >= initial, 'COUNTER_INITIAL', 'Counter starts below baseline');
        records.push({...row, time, metric: 'rainfall', value: value - initial, unit: 'mm'});
      } else {
        gaps.push({station, time, reason: 'baseline_only'});
      }
      previous.set(station, {time, value});
      continue;
    }
    const elapsed = Date.parse(time) - Date.parse(old.time);
    requireValue(elapsed > 0, 'COUNTER_DUPLICATE', 'Counter times must be unique per station');
    if (elapsed > maximumInterval) {
      gaps.push({station, from: old.time, time, reason: 'interval_exceeded'});
      previous.set(station, {time, value});
      continue;
    }
    let amount = value - old.value;
    let quality = row.quality ?? 'measured';
    if (amount < 0) {
      if (resetPolicy === 'reject') {
        gaps.push({station, time, reason: 'counter_decreased'});
        previous.set(station, {time, value});
        continue;
      }
      amount = resetPolicy === 'wrap' ? maximum - old.value + value : value;
      quality = 'estimated';
    }
    records.push({...row, time, metric: 'rainfall', value: amount, unit: 'mm', quality});
    previous.set(station, {time, value});
  }
  return {records, gaps};
}
export function alignIntervals(rows, {intervalMs, toleranceMs = 0}) {
  integer(intervalMs, 'intervalMs', 1000, 86400000);
  integer(toleranceMs, 'toleranceMs', 0, Math.floor(intervalMs / 2));
  const output = [];
  const rejected = [];
  const keys = new Set();
  for (const [index, row] of array(rows, 'rows').entries()) {
    const time = timestamp(row.time);
    const epoch = Date.parse(time);
    const nearest = Math.round(epoch / intervalMs) * intervalMs;
    if (Math.abs(nearest - epoch) > toleranceMs) {
      rejected.push({index, time, reason: 'outside_alignment_tolerance'});
      continue;
    }
    const aligned = new Date(nearest).toISOString();
    const key = [row.station, row.metric, aligned].join('|');
    requireValue(!keys.has(key), 'ALIGN_COLLISION', 'Two observations map to the same aligned slot');
    keys.add(key);
    output.push({...row, time: aligned});
  }
  return {records: output, rejected};
}
