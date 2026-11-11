import {array, canonical, choice, clone, digest, exactKeys, identifier, integer, timestamp} from './value.js';
import {LedgerError, fail, requireValue} from './errors.js';
import {normalizeMeasurement, stationAt} from './catalog.js';
import {event, observationKey, revisionEquals, applyEvent} from './state.js';

export function normalizeRecord(state, source, raw, receivedAt) {
  exactKeys(raw, ['station', 'metric', 'time', 'revision', 'operation', 'value', 'unit', 'quality'],
    ['station', 'metric', 'time', 'revision']);
  const station = identifier(raw.station, 'station');
  const metric = choice(raw.metric, source.metrics, 'metric');
  const time = timestamp(raw.time, 'observation time');
  const revision = integer(raw.revision, 'revision', 1, 1000000000);
  const operation = choice(raw.operation ?? 'upsert', ['upsert', 'delete'], 'operation');
  stationAt(state, station, source.id, time);
  requireValue(Date.parse(time) <= Date.parse(receivedAt) + source.futureSkewMs,
    'FUTURE', 'Observation exceeds source future-skew allowance');
  const quality = choice(raw.quality ?? 'measured', ['measured', 'estimated', 'suspect'], 'quality');
  let measurement = {value: null, unit: null};
  if (operation === 'upsert') {
    measurement = normalizeMeasurement(metric, raw.value, raw.unit);
  } else {
    requireValue(raw.value === undefined || raw.value === null, 'DELETE_VALUE', 'Deletion cannot carry a value');
    requireValue(raw.unit === undefined || raw.unit === null, 'DELETE_UNIT', 'Deletion cannot carry a unit');
  }
  return {
    key: observationKey(source.id, station, metric, time),
    source: source.id,
    station,
    metric,
    time,
    revision,
    operation,
    quality,
    ...measurement,
  };
}
export function checkRevision(state, record) {
  const previous = state.observations[record.key];
  if (!previous) {
    requireValue(record.operation !== 'delete', 'DELETE_MISSING', 'Cannot delete a missing observation');
    requireValue(record.revision === 1, 'REVISION_GAP', 'Initial observation must have revision one');
    return 'apply';
  }
  if (record.revision < previous.revision) {
    fail('STALE_REVISION', 'Observation revision is stale', {current: previous.revision});
  }
  if (record.revision === previous.revision) {
    if (revisionEquals(previous, record)) {
      return 'duplicate';
    }
    fail('REVISION_CONFLICT', 'Same revision has different content');
  }
  requireValue(record.revision === previous.revision + 1, 'REVISION_GAP', 'Revision must advance by one');
  return 'apply';
}
export function prepareBatch(state, input, options = {}) {
  exactKeys(input, ['id', 'source', 'receivedAt', 'mode', 'records'], ['id', 'source', 'receivedAt', 'records']);
  const id = identifier(input.id, 'batch id');
  const sourceId = identifier(input.source, 'source');
  const receivedAt = timestamp(input.receivedAt, 'receivedAt');
  const mode = choice(input.mode ?? 'partial', ['partial', 'atomic'], 'mode');
  const records = array(input.records, 'records', 10000);
  requireValue(records.length > 0, 'EMPTY_BATCH', 'Batch cannot be empty');
  const hash = digest(input);
  if (state.batches[id]) {
    requireValue(state.batches[id].hash === hash, 'BATCH_CONFLICT', 'Batch id already has different content');
    return {events: [], result: {...clone(state.batches[id].result), replay: true}};
  }
  const source = state.sources[sourceId];
  requireValue(source, 'SOURCE_MISSING', 'Unknown source');
  requireValue(source.enabled, 'SOURCE_DISABLED', 'Source is disabled');
  const projected = clone(state);
  const events = [];
  const outcomes = [];
  const counts = {accepted: 0, duplicate: 0, quarantined: 0, late: 0};
  for (let index = 0; index < records.length; index += 1) {
    const raw = records[index];
    try {
      const record = normalizeRecord(projected, source, raw, receivedAt);
      const result = checkRevision(projected, record);
      if (result === 'duplicate') {
        counts.duplicate += 1;
        outcomes.push({index, status: 'duplicate', key: record.key});
        continue;
      }
      const watermark = projected.watermarks[sourceId];
      const late = Boolean(watermark && record.time < watermark);
      if (late && !options.allowLate) {
        const age = Date.parse(watermark) - Date.parse(record.time);
        requireValue(age <= source.latenessMs, 'TOO_LATE', 'Observation is behind the correction horizon', {age});
      }
      const applied = {...record, batch: id, receivedAt, late};
      const item = event('observation.applied', applied);
      events.push(item);
      applyEvent(projected, item);
      counts.accepted += 1;
      counts.late += Number(late);
      outcomes.push({index, status: 'accepted', key: record.key, late});
    } catch (error) {
      if (!(error instanceof LedgerError)) {
        throw error;
      }
      if (mode === 'atomic') {
        fail('ATOMIC_REJECTED', 'Atomic batch rejected without committing any row', {index, code: error.code});
      }
      const quarantineId = digest({batch: id, index}).slice(0, 32);
      const quarantined = {
        id: quarantineId,
        batch: id,
        source: sourceId,
        index,
        raw: clone(raw),
        receivedAt,
        status: 'open',
        code: error.code,
        message: error.message,
        details: error.details,
      };
      events.push(event('quarantine.created', quarantined));
      counts.quarantined += 1;
      outcomes.push({index, status: 'quarantined', id: quarantineId, code: error.code});
    }
  }
  const result = {id, source: sourceId, ...counts, outcomes, replay: false};
  events.push(event('batch.committed', {id, source: sourceId, receivedAt, hash, count: records.length, result}));
  return {events, result};
}
export function advanceWatermark(state, input) {
  exactKeys(input, ['source', 'time'], ['source', 'time']);
  const source = identifier(input.source, 'source');
  requireValue(state.sources[source], 'SOURCE_MISSING', 'Unknown source');
  const time = timestamp(input.time);
  const old = state.watermarks[source];
  requireValue(!old || time >= old, 'WATERMARK_REGRESSION', 'Watermark cannot move backwards');
  if (old === time) {
    return {events: [], result: {source, time}};
  }
  return {events: [event('watermark.advanced', {source, time})], result: {source, time}};
}
export function resolveQuarantine(state, input) {
  exactKeys(input, ['id', 'action', 'record', 'actor', 'reason'], ['id', 'action', 'actor', 'reason']);
  const id = identifier(input.id);
  const item = state.quarantine[id];
  requireValue(item, 'QUARANTINE_MISSING', 'Quarantine item does not exist');
  requireValue(item.status === 'open', 'QUARANTINE_RESOLVED', 'Quarantine item is already resolved');
  const action = choice(input.action, ['discard', 'replace'], 'action');
  const actor = identifier(input.actor, 'actor');
  requireValue(typeof input.reason === 'string' && input.reason.trim().length >= 3,
    'REASON', 'Resolution requires an explanation');
  const events = [];
  if (action === 'replace') {
    const source = state.sources[item.source];
    requireValue(source.enabled, 'SOURCE_DISABLED', 'Source is disabled');
    const record = normalizeRecord(state, source, input.record, item.receivedAt);
    const result = checkRevision(state, record);
    if (result !== 'duplicate') {
      events.push(event('observation.applied', {
        ...record,
        batch: item.batch,
        receivedAt: item.receivedAt,
        late: Boolean(state.watermarks[item.source] && record.time < state.watermarks[item.source]),
        resolvedFrom: id,
      }));
    }
  }
  const resolution = {id, status: action === 'discard' ? 'discarded' : 'replaced', actor, reason: input.reason.trim()};
  events.push(event('quarantine.resolved', resolution));
  return {events, result: resolution};
}
export function batchFingerprint(input) {
  return digest(canonical(input));
}
