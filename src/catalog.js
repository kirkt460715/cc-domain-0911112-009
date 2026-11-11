import {array, choice, exactKeys, identifier, integer, number, text, timestamp, unique} from './value.js';
import {fail, requireValue} from './errors.js';
import {event} from './state.js';

export const METRICS = Object.freeze({
  rainfall: {unit: 'mm', minimum: 0, maximum: 3000, aggregation: 'sum'},
  temperature: {unit: 'C', minimum: -90, maximum: 65, aggregation: 'mean'},
  soil_moisture: {unit: 'pct', minimum: 0, maximum: 100, aggregation: 'mean'},
  humidity: {unit: 'pct', minimum: 0, maximum: 100, aggregation: 'mean'},
  wind_speed: {unit: 'm/s', minimum: 0, maximum: 150, aggregation: 'mean'},
  pressure: {unit: 'hPa', minimum: 300, maximum: 1200, aggregation: 'mean'},
});
export function metric(name) {
  choice(name, Object.keys(METRICS), 'metric');
  return METRICS[name];
}
export function normalizeMeasurement(name, value, unit) {
  const definition = metric(name);
  number(value, 'value');
  let result;
  if (unit === definition.unit) {
    result = value;
  } else if (name === 'temperature' && unit === 'F') {
    result = (value - 32) * 5 / 9;
  } else if (name === 'temperature' && unit === 'K') {
    result = value - 273.15;
  } else if (name === 'rainfall' && unit === 'in') {
    result = value * 25.4;
  } else if (name === 'wind_speed' && unit === 'km/h') {
    result = value / 3.6;
  } else if (name === 'wind_speed' && unit === 'mph') {
    result = value * 0.44704;
  } else if (name === 'pressure' && unit === 'Pa') {
    result = value / 100;
  } else if (['soil_moisture', 'humidity'].includes(name) && unit === 'fraction') {
    result = value * 100;
  } else {
    fail('UNIT', 'Unit is incompatible with metric', {metric: name, unit});
  }
  requireValue(result >= definition.minimum && result <= definition.maximum,
    'RANGE', 'Observation is outside physical limits', {metric: name, value: result});
  return {value: Math.round(result * 1e9) / 1e9, unit: definition.unit};
}
export function sourceInput(input) {
  exactKeys(input, ['id', 'name', 'region', 'metrics', 'latenessMs', 'futureSkewMs', 'priority', 'enabled'],
    ['id', 'name', 'region', 'metrics']);
  const metrics = unique(array(input.metrics, 'metrics', 6).map(name => {
    metric(name);
    return name;
  }), 'metrics');
  requireValue(metrics.length > 0, 'METRICS', 'At least one metric is required');
  if (input.enabled !== undefined) {
    requireValue(typeof input.enabled === 'boolean', 'BOOLEAN', 'enabled must be boolean');
  }
  return {
    id: identifier(input.id),
    name: text(input.name, 'name'),
    region: identifier(input.region, 'region'),
    metrics: metrics.sort(),
    latenessMs: integer(input.latenessMs ?? 86400000, 'latenessMs', 0, 31536000000),
    futureSkewMs: integer(input.futureSkewMs ?? 300000, 'futureSkewMs', 0, 86400000),
    priority: integer(input.priority ?? 100, 'priority', 0, 10000),
    enabled: input.enabled ?? true,
  };
}
export function stationInput(input) {
  exactKeys(input, ['id', 'name', 'region', 'latitude', 'longitude', 'elevation', 'intervalMs', 'sources', 'activeFrom', 'activeUntil'],
    ['id', 'name', 'region', 'latitude', 'longitude', 'sources', 'activeFrom']);
  const sources = unique(array(input.sources, 'sources', 100).map(x => identifier(x, 'source')), 'sources');
  requireValue(sources.length > 0, 'SOURCES', 'Station needs a source');
  const start = timestamp(input.activeFrom, 'activeFrom');
  const end = input.activeUntil ? timestamp(input.activeUntil, 'activeUntil') : null;
  requireValue(!end || end > start, 'STATION_PERIOD', 'Station end must follow start');
  return {
    id: identifier(input.id),
    name: text(input.name, 'name'),
    region: identifier(input.region, 'region'),
    latitude: number(input.latitude, 'latitude', -90, 90),
    longitude: number(input.longitude, 'longitude', -180, 180),
    elevation: number(input.elevation ?? 0, 'elevation', -500, 9000),
    intervalMs: integer(input.intervalMs ?? 3600000, 'intervalMs', 1000, 86400000),
    sources: sources.sort(),
    activeFrom: start,
    activeUntil: end,
  };
}
export function registerSource(state, input) {
  const source = sourceInput(input);
  requireValue(!state.sources[source.id], 'SOURCE_EXISTS', 'Source already registered');
  return {events: [event('source.registered', source)], result: {source}};
}
export function updateSource(state, id, patch) {
  identifier(id);
  const old = state.sources[id];
  requireValue(old, 'SOURCE_MISSING', 'Source not registered');
  exactKeys(patch, ['name', 'latenessMs', 'futureSkewMs', 'priority', 'enabled']);
  const source = sourceInput({...old, ...patch});
  return {events: [event('source.updated', source)], result: {source}};
}
export function registerStation(state, input) {
  const station = stationInput(input);
  requireValue(!state.stations[station.id], 'STATION_EXISTS', 'Station already registered');
  for (const id of station.sources) {
    const source = state.sources[id];
    requireValue(source, 'SOURCE_MISSING', 'Station references unregistered source', {id});
    requireValue(source.region === station.region, 'REGION', 'Source and station regions differ');
  }
  return {events: [event('station.registered', station)], result: {station}};
}
export function updateStation(state, id, patch) {
  identifier(id);
  const old = state.stations[id];
  requireValue(old, 'STATION_MISSING', 'Station not registered');
  exactKeys(patch, ['name', 'activeUntil', 'intervalMs']);
  const station = stationInput({...old, ...patch});
  if (station.activeUntil) {
    const later = Object.values(state.observations).some(item =>
      item.station === id && item.time >= station.activeUntil && item.operation !== 'delete');
    requireValue(!later, 'STATION_DATA', 'End date would invalidate existing observations');
  }
  return {events: [event('station.updated', station)], result: {station}};
}
export function stationAt(state, id, sourceId, time) {
  const station = state.stations[id];
  requireValue(station, 'STATION_MISSING', 'Unknown station', {id});
  requireValue(station.sources.includes(sourceId), 'SOURCE_STATION', 'Source is not assigned to station');
  requireValue(time >= station.activeFrom && (!station.activeUntil || time < station.activeUntil),
    'STATION_INACTIVE', 'Observation falls outside station operating period');
  return station;
}
