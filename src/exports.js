import {canonical, choice, digest, exactKeys, identifier, text} from './value.js';
import {requireValue} from './errors.js';
import {queryInput, filterObservations, preferSources, publicObservation} from './query.js';
import {encodeCsv, encodeJsonLines} from './adapters.js';
import {event} from './state.js';

export function createExport(state, input) {
  exactKeys(input, ['id', 'query', 'format', 'purpose'], ['id', 'query', 'purpose']);
  const id = identifier(input.id);
  const purpose = text(input.purpose, 'purpose', 1000);
  const format = choice(input.format ?? 'jsonl', ['jsonl', 'csv'], 'format');
  const query = queryInput(input.query);
  requireValue(!query.cursor, 'EXPORT_CURSOR', 'Exports require a full query');
  requireValue(!query.includeDeleted, 'EXPORT_DELETED', 'Exports cannot include tombstones');
  let rows = filterObservations(state, query);
  if (query.selection === 'preferred') {
    rows = preferSources(state, rows);
  }
  rows = rows.map(publicObservation);
  const columns = ['source', 'station', 'metric', 'time', 'revision', 'value', 'unit', 'quality', 'batch'];
  const content = format === 'csv' ? encodeCsv(rows, columns) : encodeJsonLines(rows);
  const lineage = rows.map(row => ({key: row.key, revision: row.revision, batch: row.batch}));
  const record = {
    id,
    purpose,
    format,
    query,
    count: rows.length,
    sequence: state.sequence,
    head: state.head,
    content,
    checksum: digest(content),
    lineage,
    revoked: false,
  };
  const previous = state.exports[id];
  if (previous) {
    requireValue(previous.checksum === record.checksum && previous.purpose === purpose && !previous.revoked,
      'EXPORT_CONFLICT', 'Export id already refers to another snapshot');
    return {events: [], result: {export: previous, replay: true}};
  }
  return {events: [event('export.created', record)], result: {export: record, replay: false}};
}
export function getExport(state, id) {
  identifier(id);
  const record = state.exports[id];
  requireValue(record, 'EXPORT_MISSING', 'Unknown export');
  requireValue(!record.revoked, 'EXPORT_REVOKED', 'Export has been revoked');
  requireValue(digest(record.content) === record.checksum, 'EXPORT_CORRUPT', 'Export checksum differs');
  const changed = [];
  for (const entry of record.lineage) {
    const current = state.observations[entry.key];
    if (!current || current.revision !== entry.revision || current.operation === 'delete') {
      changed.push({key: entry.key, exportedRevision: entry.revision, currentRevision: current?.revision ?? null});
    }
  }
  return {...record, stale: changed.length > 0, changed};
}
export function revokeExport(state, input) {
  exactKeys(input, ['id', 'reason'], ['id', 'reason']);
  const id = identifier(input.id);
  const record = state.exports[id];
  requireValue(record, 'EXPORT_MISSING', 'Unknown export');
  const reason = text(input.reason, 'reason', 1000);
  if (record.revoked) {
    return {events: [], result: {id, revoked: true}};
  }
  return {events: [event('export.revoked', {id, reason})], result: {id, revoked: true}};
}
export function exportManifest(state, id) {
  const record = getExport(state, id);
  const {content, ...manifest} = record;
  return {...manifest, bytes: Buffer.byteLength(content), manifestHash: digest(canonical(manifest))};
}
export function impactedExports(state, keys) {
  const selected = new Set(keys);
  return Object.values(state.exports).filter(record => !record.revoked &&
    record.lineage.some(entry => selected.has(entry.key))).map(record => ({
      id: record.id,
      count: record.count,
      purpose: record.purpose,
      affected: record.lineage.filter(entry => selected.has(entry.key)).length,
    }));
}
