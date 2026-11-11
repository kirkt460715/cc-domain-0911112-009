import {choice, digest, exactKeys, identifier, integer, timestamp} from './value.js';
import {requireValue, fail} from './errors.js';
import {currentObservations} from './state.js';
import {metric} from './catalog.js';

export function queryInput(input = {}) {
  exactKeys(input, ['source', 'station', 'metric', 'region', 'from', 'until', 'quality', 'includeDeleted', 'limit', 'cursor', 'selection']);
  const result = {};
  for (const key of ['source', 'station', 'region']) {
    if (input[key] !== undefined) {
      result[key] = identifier(input[key], key);
    }
  }
  if (input.metric !== undefined) {
    metric(input.metric);
    result.metric = input.metric;
  }
  if (input.from !== undefined) {
    result.from = timestamp(input.from, 'from');
  }
  if (input.until !== undefined) {
    result.until = timestamp(input.until, 'until');
  }
  requireValue(!result.from || !result.until || result.from < result.until,
    'PERIOD', 'Query end must follow start');
  if (input.quality !== undefined) {
    result.quality = choice(input.quality, ['measured', 'estimated', 'suspect'], 'quality');
  }
  if (input.includeDeleted !== undefined) {
    requireValue(typeof input.includeDeleted === 'boolean', 'BOOLEAN', 'includeDeleted must be boolean');
  }
  result.includeDeleted = input.includeDeleted ?? false;
  result.selection = choice(input.selection ?? 'all', ['all', 'preferred'], 'selection');
  result.limit = integer(input.limit ?? 1000, 'limit', 1, 10000);
  result.cursor = input.cursor ?? null;
  return result;
}
export function compareObservation(a, b) {
  return a.time.localeCompare(b.time) || a.station.localeCompare(b.station) ||
    a.metric.localeCompare(b.metric) || a.source.localeCompare(b.source);
}
export function filterObservations(state, query) {
  const observations = query.includeDeleted ? Object.values(state.observations) : currentObservations(state);
  return observations.filter(item => {
    if (query.source && item.source !== query.source) {
      return false;
    }
    if (query.station && item.station !== query.station) {
      return false;
    }
    if (query.metric && item.metric !== query.metric) {
      return false;
    }
    if (query.region && state.stations[item.station].region !== query.region) {
      return false;
    }
    if (query.from && item.time < query.from) {
      return false;
    }
    if (query.until && item.time >= query.until) {
      return false;
    }
    if (query.quality && item.quality !== query.quality) {
      return false;
    }
    return true;
  }).sort(compareObservation);
}
export function preferSources(state, observations) {
  const groups = new Map();
  const qualityRank = {measured: 0, estimated: 1, suspect: 2};
  for (const observation of observations) {
    const key = [observation.station, observation.metric, observation.time].join('|');
    const previous = groups.get(key);
    const score = [qualityRank[observation.quality], state.sources[observation.source].priority, observation.source];
    if (!previous) {
      groups.set(key, {observation, score});
      continue;
    }
    let better = false;
    for (let index = 0; index < score.length; index += 1) {
      if (score[index] !== previous.score[index]) {
        better = score[index] < previous.score[index];
        break;
      }
    }
    if (better) {
      groups.set(key, {observation, score});
    }
  }
  return Array.from(groups.values(), item => item.observation).sort(compareObservation);
}
export function publicObservation(item) {
  const {history, ...observation} = item;
  return observation;
}
export function queryObservations(state, input = {}) {
  const query = queryInput(input);
  let values = filterObservations(state, query);
  if (query.selection === 'preferred') {
    values = preferSources(state, values);
  }
  const {cursor, ...identity} = query;
  const queryHash = digest(identity);
  let offset = 0;
  if (cursor) {
    let decoded;
    try {
      decoded = JSON.parse(Buffer.from(cursor, 'base64url').toString());
    } catch {
      fail('CURSOR', 'Invalid pagination cursor');
    }
    requireValue(decoded.head === state.head, 'CURSOR_STALE', 'Dataset changed; restart pagination');
    requireValue(decoded.query === queryHash, 'CURSOR_QUERY', 'Cursor belongs to another query');
    offset = integer(decoded.offset, 'cursor offset', 0, values.length);
  }
  const rows = values.slice(offset, offset + query.limit).map(publicObservation);
  const nextOffset = offset + rows.length;
  const next = nextOffset < values.length ? Buffer.from(JSON.stringify({
    head: state.head, query: queryHash, offset: nextOffset,
  })).toString('base64url') : null;
  return {sequence: state.sequence, total: values.length, rows, next};
}
export function observationHistory(state, key) {
  const observation = state.observations[key];
  requireValue(observation, 'OBSERVATION_MISSING', 'Unknown observation');
  return {key, revision: observation.revision, history: structuredClone(observation.history)};
}
export function timeline(state, query = {}) {
  const normalized = queryInput({...query, limit: 10000});
  const rows = filterObservations(state, normalized);
  const result = [];
  for (const item of rows) {
    for (const revision of item.history) {
      result.push({
        key: item.key,
        revision: revision.revision,
        receivedAt: revision.receivedAt,
        time: revision.time,
        operation: revision.operation,
        batch: revision.batch,
        late: revision.late,
      });
    }
  }
  return result.sort((a, b) => a.receivedAt.localeCompare(b.receivedAt) || a.key.localeCompare(b.key) || a.revision - b.revision);
}
