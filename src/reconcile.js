import {exactKeys, identifier, integer, array, digest, timestamp} from './value.js';
import {requireValue} from './errors.js';
import {filterObservations, queryInput} from './query.js';
import {observationKey} from './state.js';
import {impactedExports} from './exports.js';

export function inventory(state, input) {
  const query = queryInput(input);
  const rows = filterObservations(state, query);
  const entries = rows.map(row => ({
    key: row.key,
    source: row.source,
    station: row.station,
    metric: row.metric,
    time: row.time,
    revision: row.revision,
    digest: digest({value: row.value, unit: row.unit, quality: row.quality, operation: row.operation}),
  }));
  return {version: 1, head: state.head, sequence: state.sequence, entries, digest: digest(entries)};
}
export function compareInventory(state, input) {
  exactKeys(input, ['query', 'remote'], ['query', 'remote']);
  const local = inventory(state, input.query);
  const remote = array(input.remote, 'remote', 100000);
  const indexed = new Map();
  for (const entry of remote) {
    exactKeys(entry, ['key', 'source', 'station', 'metric', 'time', 'revision', 'digest'],
      ['key', 'source', 'station', 'metric', 'time', 'revision', 'digest']);
    identifier(entry.source);
    identifier(entry.station);
    identifier(entry.metric);
    timestamp(entry.time);
    integer(entry.revision, 'revision', 1);
    requireValue(entry.key === observationKey(entry.source, entry.station, entry.metric, entry.time),
      'INVENTORY_KEY', 'Inventory key does not match its fields');
    requireValue(typeof entry.digest === 'string' && /^[a-f0-9]{64}$/.test(entry.digest),
      'INVENTORY_DIGEST', 'Inventory digest is invalid');
    requireValue(!indexed.has(entry.key), 'INVENTORY_DUPLICATE', 'Remote inventory repeats a key');
    indexed.set(entry.key, entry);
  }
  const missingLocally = [];
  const missingRemotely = [];
  const conflicts = [];
  const ahead = [];
  const behind = [];
  let equal = 0;
  for (const entry of local.entries) {
    const other = indexed.get(entry.key);
    if (!other) {
      missingRemotely.push(entry);
      continue;
    }
    indexed.delete(entry.key);
    if (entry.revision > other.revision) {
      ahead.push({key: entry.key, local: entry.revision, remote: other.revision});
    } else if (entry.revision < other.revision) {
      behind.push({key: entry.key, local: entry.revision, remote: other.revision});
    } else if (entry.digest !== other.digest) {
      conflicts.push({key: entry.key, revision: entry.revision, local: entry.digest, remote: other.digest});
    } else {
      equal += 1;
    }
  }
  missingLocally.push(...indexed.values());
  return {sequence: state.sequence, equal, missingLocally, missingRemotely, conflicts, ahead, behind};
}
export function correctionImpact(state, input) {
  exactKeys(input, ['keys'], ['keys']);
  const keys = array(input.keys, 'keys', 10000);
  requireValue(new Set(keys).size === keys.length, 'DUPLICATE', 'Impact keys contain duplicates');
  const observations = [];
  for (const key of keys) {
    const row = state.observations[key];
    requireValue(row, 'OBSERVATION_MISSING', 'Impact key does not exist', {key});
    observations.push({key, revision: row.revision, operation: row.operation, time: row.time});
  }
  const selected = new Set(keys);
  const alerts = Object.values(state.alerts).filter(alert => alert.lineage.some(entry => selected.has(entry.key)))
    .map(alert => ({id: alert.id, rule: alert.rule, generation: alert.generation, active: alert.active}));
  return {sequence: state.sequence, observations, exports: impactedExports(state, keys), alerts};
}
export function auditState(state) {
  const findings = [];
  for (const observation of Object.values(state.observations)) {
    if (!state.sources[observation.source]) {
      findings.push({code: 'ORPHAN_SOURCE', key: observation.key});
    }
    if (!state.stations[observation.station]) {
      findings.push({code: 'ORPHAN_STATION', key: observation.key});
    }
    if (!state.batches[observation.batch]) {
      findings.push({code: 'ORPHAN_BATCH', key: observation.key});
    }
    let expected = 1;
    for (const revision of observation.history) {
      if (revision.revision !== expected) {
        findings.push({code: 'REVISION_CHAIN', key: observation.key, expected, actual: revision.revision});
      }
      expected = revision.revision + 1;
    }
    if (observation.history.at(-1)?.revision !== observation.revision) {
      findings.push({code: 'REVISION_HEAD', key: observation.key});
    }
  }
  for (const batch of Object.values(state.batches)) {
    const result = batch.result;
    if (result.accepted + result.duplicate + result.quarantined !== batch.count) {
      findings.push({code: 'BATCH_COUNTS', id: batch.id});
    }
  }
  for (const item of Object.values(state.quarantine)) {
    if (!state.batches[item.batch]) {
      findings.push({code: 'QUARANTINE_BATCH', id: item.id});
    }
    if (item.status !== 'open' && !item.resolution) {
      findings.push({code: 'QUARANTINE_RESOLUTION', id: item.id});
    }
  }
  for (const record of Object.values(state.exports)) {
    if (digest(record.content) !== record.checksum) {
      findings.push({code: 'EXPORT_CHECKSUM', id: record.id});
    }
    for (const entry of record.lineage) {
      const current = state.observations[entry.key];
      if (!current || !current.history.some(revision => revision.revision === entry.revision)) {
        findings.push({code: 'EXPORT_LINEAGE', id: record.id, key: entry.key});
      }
    }
  }
  return {valid: findings.length === 0, sequence: state.sequence, findings};
}
