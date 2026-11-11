import {timestamp, integer, identifier, exactKeys} from './value.js';
import {requireValue} from './errors.js';
import {summary, currentObservations} from './state.js';
import {profile, freshness} from './quality.js';
import {auditState} from './reconcile.js';

export function operationsReport(state, input) {
  exactKeys(input, ['at', 'freshnessMs'], ['at']);
  const at = timestamp(input.at);
  const freshnessMs = integer(input.freshnessMs ?? 86400000, 'freshnessMs', 1);
  const status = summary(state);
  const recent = freshness(state, {at, maximumAgeMs: freshnessMs});
  const byCode = {};
  const bySource = {};
  for (const item of Object.values(state.quarantine)) {
    if (item.status !== 'open') {
      continue;
    }
    byCode[item.code] = (byCode[item.code] ?? 0) + 1;
    bySource[item.source] = (bySource[item.source] ?? 0) + 1;
  }
  const disabled = Object.values(state.sources).filter(source => !source.enabled).map(source => source.id);
  const unacknowledged = Object.values(state.alerts).filter(alert => alert.active &&
    (!alert.acknowledgement || alert.acknowledgement.generation !== alert.generation));
  const staleExports = [];
  for (const record of Object.values(state.exports)) {
    if (record.revoked) {
      continue;
    }
    const changed = record.lineage.filter(entry => state.observations[entry.key]?.revision !== entry.revision);
    if (changed.length) {
      staleExports.push({id: record.id, changed: changed.length});
    }
  }
  const health = auditState(state);
  return {
    at,
    status,
    integrity: health,
    freshness: recent,
    quarantine: {byCode, bySource},
    disabledSources: disabled,
    unacknowledgedAlerts: unacknowledged.map(alert => ({id: alert.id, generation: alert.generation, rule: alert.rule})),
    staleExports,
    attentionRequired: !health.valid || recent.sources.some(item => item.stale) ||
      status.quarantined > 0 || unacknowledged.length > 0 || staleExports.length > 0,
  };
}
export function regionReport(state, input) {
  exactKeys(input, ['region', 'from', 'until'], ['region', 'from', 'until']);
  const region = identifier(input.region);
  const from = timestamp(input.from);
  const until = timestamp(input.until);
  requireValue(from < until, 'PERIOD', 'Region report end must follow start');
  const stations = Object.values(state.stations).filter(station => station.region === region);
  const sourceIds = new Set(stations.flatMap(station => station.sources));
  const rows = currentObservations(state).filter(row =>
    state.stations[row.station].region === region && row.time >= from && row.time < until);
  const metrics = {};
  for (const row of rows) {
    if (!metrics[row.metric]) {
      metrics[row.metric] = {observations: 0, stations: new Set(), sources: new Set(), estimated: 0, suspect: 0};
    }
    const group = metrics[row.metric];
    group.observations += 1;
    group.stations.add(row.station);
    group.sources.add(row.source);
    group.estimated += Number(row.quality === 'estimated');
    group.suspect += Number(row.quality === 'suspect');
  }
  return {
    region,
    from,
    until,
    sequence: state.sequence,
    stations: stations.length,
    sources: sourceIds.size,
    observations: rows.length,
    metrics: Object.fromEntries(Object.entries(metrics).map(([name, value]) => [name, {
      ...value, stations: value.stations.size, sources: value.sources.size,
    }])),
    profiles: profile(state, {region, from, until}).groups,
  };
}
export function sourceScorecard(state, sourceId) {
  identifier(sourceId);
  const source = state.sources[sourceId];
  requireValue(source, 'SOURCE_MISSING', 'Unknown source');
  const batches = Object.values(state.batches).filter(batch => batch.source === sourceId);
  const totals = {submitted: 0, accepted: 0, duplicate: 0, quarantined: 0, late: 0};
  for (const batch of batches) {
    totals.submitted += batch.count;
    totals.accepted += batch.result.accepted;
    totals.duplicate += batch.result.duplicate;
    totals.quarantined += batch.result.quarantined;
    totals.late += batch.result.late;
  }
  const open = Object.values(state.quarantine).filter(item => item.source === sourceId && item.status === 'open').length;
  return {
    source: sourceId,
    name: source.name,
    enabled: source.enabled,
    batchCount: batches.length,
    ...totals,
    openQuarantine: open,
    acceptanceRate: totals.submitted ? totals.accepted / totals.submitted : null,
    duplicateRate: totals.submitted ? totals.duplicate / totals.submitted : null,
    correctionRate: totals.accepted ? currentObservations(state).filter(row =>
      row.source === sourceId && row.revision > 1).length / totals.accepted : null,
    watermark: state.watermarks[sourceId] ?? null,
  };
}
