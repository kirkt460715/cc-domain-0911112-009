import {requireValue} from './errors.js';
import {digest, integer} from './value.js';
import {emptyState, applyTransaction} from './state.js';

export function auditTransactions(transactions) {
  const findings = [];
  const actors = new Map();
  let mutations = 0;
  for (const transaction of transactions) {
    const auditEvents = transaction.events.filter(item => item.type === 'audit.recorded');
    const businessEvents = transaction.events.filter(item => item.type !== 'audit.recorded');
    if (auditEvents.length !== 1) {
      findings.push({sequence: transaction.sequence, code: 'AUDIT_COUNT', actual: auditEvents.length});
      continue;
    }
    const audit = auditEvents[0].data;
    if (audit.sequence !== transaction.sequence) {
      findings.push({sequence: transaction.sequence, code: 'AUDIT_SEQUENCE'});
    }
    if (audit.fingerprint !== digest(businessEvents)) {
      findings.push({sequence: transaction.sequence, code: 'AUDIT_FINGERPRINT'});
    }
    if (audit.eventCount !== businessEvents.length) {
      findings.push({sequence: transaction.sequence, code: 'AUDIT_EVENTS'});
    }
    const aggregate = actors.get(audit.actor) ?? {transactions: 0, mutations: 0, operations: {}};
    aggregate.transactions += 1;
    aggregate.mutations += businessEvents.length;
    aggregate.operations[audit.operation] = (aggregate.operations[audit.operation] ?? 0) + 1;
    actors.set(audit.actor, aggregate);
    mutations += businessEvents.length;
  }
  return {
    valid: findings.length === 0,
    transactions: transactions.length,
    mutations,
    actors: Object.fromEntries([...actors.entries()].sort(([a], [b]) => a.localeCompare(b))),
    findings,
  };
}
export function stateAtSequence(transactions, sequence) {
  integer(sequence, 'sequence', 0, transactions.length);
  const state = emptyState();
  for (const transaction of transactions) {
    if (transaction.sequence > sequence) {
      break;
    }
    applyTransaction(state, transaction);
  }
  requireValue(state.sequence === sequence, 'SEQUENCE_MISSING', 'Requested historical sequence is unavailable');
  return state;
}
export function lineageGraph(state, key) {
  const observation = state.observations[key];
  requireValue(observation, 'OBSERVATION_MISSING', 'Unknown observation');
  const nodes = [];
  const edges = [];
  let predecessor = null;
  for (const revision of observation.history) {
    const id = `${key}@${revision.revision}`;
    nodes.push({id, type: 'revision', revision: revision.revision, operation: revision.operation, value: revision.value});
    const batchId = `batch:${revision.batch}`;
    if (!nodes.some(node => node.id === batchId)) {
      nodes.push({id: batchId, type: 'batch', source: revision.source, receivedAt: revision.receivedAt});
    }
    edges.push({from: batchId, to: id, relationship: 'supplied'});
    if (predecessor) {
      edges.push({from: predecessor, to: id, relationship: 'superseded_by'});
    }
    predecessor = id;
  }
  for (const record of Object.values(state.exports)) {
    const entry = record.lineage.find(item => item.key === key);
    if (entry) {
      const id = `export:${record.id}`;
      nodes.push({id, type: 'export', revoked: record.revoked, checksum: record.checksum});
      edges.push({from: `${key}@${entry.revision}`, to: id, relationship: 'included_in'});
    }
  }
  for (const alert of Object.values(state.alerts)) {
    const entry = alert.lineage.find(item => item.key === key);
    if (entry) {
      const id = `alert:${alert.id}`;
      nodes.push({id, type: 'alert', active: alert.active, generation: alert.generation});
      edges.push({from: `${key}@${entry.revision}`, to: id, relationship: 'trigger_input'});
    }
  }
  return {key, sequence: state.sequence, nodes, edges};
}
