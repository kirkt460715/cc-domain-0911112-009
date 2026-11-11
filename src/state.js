import {clone, digest, same} from './value.js';
import {fail} from './errors.js';

export function emptyState() {
  return {
    version: 1,
    sequence: 0,
    head: '0'.repeat(64),
    sources: {},
    stations: {},
    batches: {},
    observations: {},
    quarantine: {},
    watermarks: {},
    rules: {},
    alerts: {},
    exports: {},
    audits: [],
  };
}
function lookup(state, collection, id) {
  const item = state[collection][id];
  if (!item) {
    fail('JOURNAL_INVARIANT', `Missing ${collection} entry`, {id}, 500);
  }
  return item;
}
export function applyEvent(state, event) {
  const {type, data} = event;
  switch (type) {
    case 'source.registered':
      state.sources[data.id] = clone(data);
      break;
    case 'source.updated':
      lookup(state, 'sources', data.id);
      state.sources[data.id] = clone(data);
      break;
    case 'station.registered':
      state.stations[data.id] = clone(data);
      break;
    case 'station.updated':
      lookup(state, 'stations', data.id);
      state.stations[data.id] = clone(data);
      break;
    case 'batch.committed':
      if (state.batches[data.id]) {
        fail('JOURNAL_INVARIANT', 'Batch committed twice', {id: data.id}, 500);
      }
      state.batches[data.id] = clone(data);
      break;
    case 'observation.applied': {
      const old = state.observations[data.key];
      const history = old ? clone(old.history) : [];
      history.push(clone(data));
      state.observations[data.key] = {...clone(data), history};
      break;
    }
    case 'quarantine.created':
      state.quarantine[data.id] = clone(data);
      break;
    case 'quarantine.resolved': {
      const item = lookup(state, 'quarantine', data.id);
      item.status = data.status;
      item.resolution = clone(data);
      break;
    }
    case 'watermark.advanced':
      state.watermarks[data.source] = data.time;
      break;
    case 'rule.saved':
      state.rules[data.id] = clone(data);
      break;
    case 'rule.disabled':
      lookup(state, 'rules', data.id).enabled = false;
      break;
    case 'alert.updated':
      state.alerts[data.id] = clone(data);
      break;
    case 'alert.acknowledged': {
      const alert = lookup(state, 'alerts', data.id);
      alert.acknowledgement = clone(data);
      break;
    }
    case 'export.created':
      state.exports[data.id] = clone(data);
      break;
    case 'export.revoked': {
      const record = lookup(state, 'exports', data.id);
      record.revoked = true;
      record.reason = data.reason;
      break;
    }
    case 'audit.recorded':
      state.audits.push(clone(data));
      break;
    default:
      fail('JOURNAL_EVENT', 'Unknown event in journal', {type}, 500);
  }
}
export function applyTransaction(state, transaction) {
  if (transaction.sequence !== state.sequence + 1 || transaction.previous !== state.head) {
    fail('JOURNAL_CHAIN', 'Journal sequence or predecessor mismatch', {}, 500);
  }
  const {hash, ...body} = transaction;
  if (digest(body) !== hash) {
    fail('JOURNAL_HASH', 'Journal transaction checksum mismatch', {}, 500);
  }
  for (const event of transaction.events) {
    applyEvent(state, event);
  }
  state.sequence = transaction.sequence;
  state.head = hash;
  return state;
}
export function event(type, data) {
  return {type, data};
}
export function projectEvents(state, events) {
  const next = clone(state);
  for (const item of events) {
    applyEvent(next, item);
  }
  return next;
}
export function observationKey(source, station, metric, time) {
  return [source, station, metric, time].join('|');
}
export function currentObservations(state) {
  return Object.values(state.observations).filter(item => item.operation !== 'delete');
}
export function revisionEquals(left, right) {
  const fields = ['key', 'revision', 'operation', 'value', 'quality', 'unit'];
  return fields.every(field => same(left[field] ?? null, right[field] ?? null));
}
export function summary(state) {
  const current = currentObservations(state);
  return {
    sequence: state.sequence,
    head: state.head,
    sources: Object.keys(state.sources).length,
    stations: Object.keys(state.stations).length,
    batches: Object.keys(state.batches).length,
    activeObservations: current.length,
    deletedObservations: Object.keys(state.observations).length - current.length,
    revisions: Object.values(state.observations).reduce((sum, item) => sum + item.history.length, 0),
    quarantined: Object.values(state.quarantine).filter(item => item.status === 'open').length,
    activeAlerts: Object.values(state.alerts).filter(item => item.active).length,
    exports: Object.keys(state.exports).length,
  };
}
