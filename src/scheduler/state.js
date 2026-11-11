import {clone} from '../value.js';
import {fail, requireValue} from '../errors.js';
import {OPEN_STATUSES, TERMINAL_STATUSES} from './model.js';

export function emptySchedulerState() {
  return {
    version: 1,
    sequence: 0,
    head: '0'.repeat(64),
    tasks: {},
    requests: [],
    rateHits: {host: {}, group: {}},
    sources: {},
    budget: {limit: 0, reserved: 0, spent: 0},
    rejected: [],
    counters: {enqueued: 0, completed: 0, permanentFailed: 0, exhausted: 0, rateLimited: 0, retried: 0, stale: 0},
    config: null,
  };
}

const TERMINAL = new Set(TERMINAL_STATUSES);
const OPEN = new Set(OPEN_STATUSES);

function task(state, id) {
  const item = state.tasks[id];
  requireValue(item, 'TASK_MISSING', 'Task does not exist', {id});
  return item;
}
function requireOpen(item) {
  requireValue(OPEN.has(item.status), 'TASK_STATE', 'Task is no longer open', {id: item.id, status: item.status});
  return item;
}
function requireTerminal(item) {
  requireValue(TERMINAL.has(item.status), 'TASK_STATE', 'Task is not terminal', {id: item.id, status: item.status});
  return item;
}
function releaseReservation(state, item) {
  state.budget.reserved -= item.estimatedCost;
  if (state.budget.reserved < 0) {
    fail('JOURNAL_INVARIANT', 'Reserved budget became negative', {}, 500);
  }
}
function finish(state, item, status) {
  releaseReservation(state, item);
  item.status = status;
  item.endedAt = state.lastEventAt;
}
function pruneRequests(state, nowMs, windowMs) {
  const horizon = nowMs - windowMs;
  state.requests = state.requests.filter(entry => entry.atMs >= horizon);
  for (const bucket of Object.values(state.rateHits)) {
    for (const [key, times] of Object.entries(bucket)) {
      const kept = times.filter(at => at >= horizon);
      if (kept.length) {
        bucket[key] = kept;
      } else {
        delete bucket[key];
      }
    }
  }
}
function pruneRejected(state) {
  if (state.rejected.length > 200) {
    state.rejected = state.rejected.slice(-200);
  }
}

export function applySchedulerEvent(state, event, nowMs = Date.now()) {
  const {type, data} = event;
  switch (type) {
    case 'scheduler.configured': {
      requireValue(!state.config, 'CONFIG_STATE', 'Scheduler configuration is already set');
      state.config = clone(data.config);
      state.budget.limit = state.config.budget;
      break;
    }
    case 'task.enqueued': {
      const record = clone(data.task);
      requireValue(!state.tasks[record.id], 'TASK_EXISTS', 'Task id is already queued');
      requireValue(state.config, 'CONFIG_MISSING', 'Configure the scheduler before enqueueing');
      requireValue(!state.sources[record.source] || state.sources[record.source].paused === false,
        'SOURCE_PAUSED', 'Source is paused; resume it before enqueueing');
      requireValue(state.budget.reserved + state.budget.spent + record.estimatedCost <= state.budget.limit,
        'BUDGET_EXCEEDED', 'Task would exceed the shared budget',
        {limit: state.budget.limit, reserved: state.budget.reserved, spent: state.budget.spent});
      const item = {
        ...record,
        status: 'queued',
        attempts: 0,
        fencing: 0,
        lease: null,
        notBefore: null,
        cursor: null,
        pages: 0,
        lastError: null,
        batchId: null,
        enqueuedAt: data.at,
        updatedAt: data.at,
        endedAt: null,
      };
      state.budget.reserved += item.estimatedCost;
      state.tasks[item.id] = item;
      state.counters.enqueued += 1;
      break;
    }
    case 'task.leased': {
      const item = requireOpen(task(state, data.id));
      requireValue(['queued', 'waiting_retry'].includes(item.status), 'TASK_STATE',
        'Only queued or retry-waiting tasks can be leased', {id: item.id, status: item.status});
      item.status = 'leased';
      item.attempts += 1;
      item.fencing += 1;
      item.lease = {owner: data.owner, leasedAt: data.at, expiresAt: data.expiresAt, attempt: item.attempts};
      item.notBefore = null;
      item.updatedAt = data.at;
      break;
    }
    case 'task.retryScheduled': {
      const item = requireOpen(task(state, data.id));
      requireValue(item.fencing === data.fencing, 'FENCING', 'Lease changed before retry was recorded',
        {id: item.id});
      requireValue(item.status === 'leased', 'TASK_STATE', 'Only leased tasks can enter retry waiting',
        {id: item.id, status: item.status});
      item.status = 'waiting_retry';
      item.lease = null;
      item.notBefore = data.notBefore;
      item.lastError = clone(data.error);
      item.updatedAt = data.at;
      state.counters.retried += 1;
      if (data.error?.kind === 'rate_limited') {
        state.counters.rateLimited += 1;
      }
      break;
    }
    case 'task.pageAdvanced': {
      const item = requireOpen(task(state, data.id));
      requireValue(item.fencing === data.fencing, 'FENCING', 'Lease changed before the page was recorded',
        {id: item.id});
      requireValue(item.status === 'leased', 'TASK_STATE', 'Only leased tasks can advance pages',
        {id: item.id, status: item.status});
      item.cursor = data.cursor;
      item.pages += 1;
      item.updatedAt = data.at;
      break;
    }
    case 'task.completed': {
      const item = requireOpen(task(state, data.id));
      requireValue(item.fencing === data.fencing, 'FENCING', 'Lease changed before completion',
        {id: item.id});
      requireValue(item.status === 'leased', 'TASK_STATE', 'Only leased tasks can complete',
        {id: item.id, status: item.status});
      finish(state, item, 'completed');
      item.batchId = data.batchId;
      item.updatedAt = data.at;
      state.budget.spent += item.estimatedCost;
      state.counters.completed += 1;
      break;
    }
    case 'task.permanentlyFailed': {
      const item = requireOpen(task(state, data.id));
      requireValue(item.fencing === data.fencing, 'FENCING', 'Lease changed before the failure was recorded',
        {id: item.id});
      requireValue(item.status === 'leased', 'TASK_STATE', 'Only leased tasks can fail permanently',
        {id: item.id, status: item.status});
      finish(state, item, 'permanent_failed');
      item.lastError = clone(data.error);
      item.updatedAt = data.at;
      state.counters.permanentFailed += 1;
      break;
    }
    case 'task.exhausted': {
      const item = requireOpen(task(state, data.id));
      requireValue(item.fencing === data.fencing, 'FENCING', 'Lease changed before exhaustion',
        {id: item.id});
      requireValue(item.status === 'leased', 'TASK_STATE', 'Only leased tasks can be exhausted',
        {id: item.id, status: item.status});
      finish(state, item, 'exhausted');
      item.lastError = clone(data.error);
      item.updatedAt = data.at;
      state.counters.exhausted += 1;
      break;
    }
    case 'source.paused': {
      const current = state.sources[data.source] ?? {paused: false, since: null};
      requireValue(current.paused === false, 'SOURCE_STATE', 'Source is already paused', {source: data.source});
      state.sources[data.source] = {paused: true, since: data.at};
      for (const revoked of data.revoked) {
        const item = state.tasks[revoked.id];
        requireValue(item && item.status === 'leased' && item.fencing === revoked.fencing,
          'FENCING', 'Paused task lease no longer matches', {id: revoked.id});
        item.status = 'paused';
        item.lease = null;
        item.fencing += 1;
        item.attempts += 1;
        item.updatedAt = data.at;
      }
      break;
    }
    case 'source.resumed': {
      const current = state.sources[data.source];
      requireValue(current && current.paused, 'SOURCE_STATE', 'Source is not paused', {source: data.source});
      state.sources[data.source] = {paused: false, since: null};
      for (const id of data.tasks) {
        const item = state.tasks[id];
        requireValue(item && item.status === 'paused', 'TASK_STATE', 'Resumed task is not paused', {id});
        item.status = 'queued';
        item.updatedAt = data.at;
      }
      break;
    }
    case 'lease.takenOver': {
      const item = requireOpen(task(state, data.id));
      requireValue(item.status === 'leased', 'TASK_STATE', 'Only leased tasks can be taken over',
        {id: item.id, status: item.status});
      requireValue(item.fencing === data.fencing, 'FENCING', 'Lease generation no longer matches',
        {id: item.id});
      item.status = 'queued';
      item.lease = null;
      item.fencing += 1;
      item.updatedAt = data.at;
      break;
    }
    case 'request.recorded': {
      state.requests.push(clone(data.entry));
      if (data.entry.dimension === 'host') {
        const bucket = state.rateHits.host[data.entry.key] ?? [];
        bucket.push(data.entry.atMs);
        state.rateHits.host[data.entry.key] = bucket;
      } else {
        const bucket = state.rateHits.group[data.entry.key] ?? [];
        bucket.push(data.entry.atMs);
        state.rateHits.group[data.entry.key] = bucket;
      }
      break;
    }
    case 'progress.rejected': {
      state.counters.stale += 1;
      state.rejected.push({at: data.at, task: data.id, reason: data.reason, fencing: data.fencing});
      pruneRejected(state);
      break;
    }
    default:
      fail('JOURNAL_EVENT', 'Unknown scheduler event', {type}, 500);
  }
  pruneRequests(state, nowMs, state.config ? Math.max(1000, state.config.leaseMs) : 1000);
  state.lastEventAt = new Date(nowMs).toISOString();
  return state;
}

function countInWindow(times, nowMs, windowMs) {
  const horizon = nowMs - windowMs;
  let count = 0;
  for (const at of times) {
    if (at >= horizon) {
      count += 1;
    }
  }
  return count;
}

function dispatchCounts(state) {
  const counts = new Map();
  for (const entry of state.requests) {
    if (entry.dimension !== 'group') {
      continue;
    }
    const key = `${entry.credentialGroup}|${entry.source}`;
    counts.set(key, (counts.get(key) ?? 0) + 1);
  }
  return counts;
}

export function selectNextTasks(state, nowMs = Date.now(), limit = Number.MAX_SAFE_INTEGER) {
  requireValue(state.config, 'CONFIG_MISSING', 'Scheduler has no configuration');
  const config = state.config;
  const windowMs = 1000;
  const inflight = {global: 0, host: {}, group: {}, source: {}};
  for (const item of Object.values(state.tasks)) {
    if (item.status !== 'leased') {
      continue;
    }
    inflight.global += 1;
    inflight.host[item.host] = (inflight.host[item.host] ?? 0) + 1;
    inflight.group[item.credentialGroup] = (inflight.group[item.credentialGroup] ?? 0) + 1;
    inflight.source[item.source] = (inflight.source[item.source] ?? 0) + 1;
  }
  const fairness = dispatchCounts(state);
  const candidates = Object.values(state.tasks).filter(item =>
    (item.status === 'queued' || (item.status === 'waiting_retry' && Date.parse(item.notBefore) <= nowMs)) &&
    !state.sources[item.source]?.paused);
  candidates.sort((a, b) => {
    if (b.priority !== a.priority) {
      return b.priority - a.priority;
    }
    const fairA = fairness.get(`${a.credentialGroup}|${a.source}`) ?? 0;
    const fairB = fairness.get(`${b.credentialGroup}|${b.source}`) ?? 0;
    if (fairA !== fairB) {
      return fairA - fairB;
    }
    return a.id.localeCompare(b.id, 'en');
  });
  const chosen = [];
  const reservations = {host: {}, group: {}, source: {}};
  for (const item of candidates) {
    if (chosen.length >= limit) {
      break;
    }
    const hostInFlight = (inflight.host[item.host] ?? 0) + (reservations.host[item.host] ?? 0);
    const groupInFlight = (inflight.group[item.credentialGroup] ?? 0) + (reservations.group[item.credentialGroup] ?? 0);
    const sourceInFlight = (inflight.source[item.source] ?? 0) + (reservations.source[item.source] ?? 0);
    if (hostInFlight >= config.hostConcurrency ||
      groupInFlight >= config.groupConcurrency ||
      sourceInFlight >= config.sourceConcurrency) {
      continue;
    }
    if (config.hostRatePerSecond !== null) {
      const used = countInWindow(state.rateHits.host[item.host] ?? [], nowMs, windowMs) +
        (reservations.host[item.host] ?? 0);
      if (used >= config.hostRatePerSecond) {
        continue;
      }
    }
    if (config.groupRatePerSecond !== null) {
      const used = countInWindow(state.rateHits.group[item.credentialGroup] ?? [], nowMs, windowMs) +
        (reservations.group[item.credentialGroup] ?? 0);
      if (used >= config.groupRatePerSecond) {
        continue;
      }
    }
    chosen.push(item);
    reservations.host[item.host] = (reservations.host[item.host] ?? 0) + 1;
    reservations.group[item.credentialGroup] = (reservations.group[item.credentialGroup] ?? 0) + 1;
    reservations.source[item.source] = (reservations.source[item.source] ?? 0) + 1;
  }
  return chosen.map(item => item.id);
}

export function describeBlocked(state, nowMs = Date.now()) {
  requireValue(state.config, 'CONFIG_MISSING', 'Scheduler has no configuration');
  const inflight = {global: 0, host: {}, group: {}, source: {}};
  for (const item of Object.values(state.tasks)) {
    if (item.status !== 'leased') {
      continue;
    }
    inflight.global += 1;
    inflight.host[item.host] = (inflight.host[item.host] ?? 0) + 1;
    inflight.group[item.credentialGroup] = (inflight.group[item.credentialGroup] ?? 0) + 1;
    inflight.source[item.source] = (inflight.source[item.source] ?? 0) + 1;
  }
  const reasons = [];
  for (const item of Object.values(state.tasks)) {
    if (!['queued', 'waiting_retry'].includes(item.status) || state.sources[item.source]?.paused) {
      continue;
    }
    let reason = null;
    if (item.status === 'waiting_retry' && Date.parse(item.notBefore) > nowMs) {
      reason = 'retry_wait';
    } else if (inflight.global >= state.config.maxConcurrency) {
      reason = 'global_concurrency';
    } else if ((inflight.host[item.host] ?? 0) >= state.config.hostConcurrency) {
      reason = 'host_concurrency';
    } else if ((inflight.group[item.credentialGroup] ?? 0) >= state.config.groupConcurrency) {
      reason = 'group_concurrency';
    } else if ((inflight.source[item.source] ?? 0) >= state.config.sourceConcurrency) {
      reason = 'source_concurrency';
    }
    if (reason) {
      reasons.push({id: item.id, source: item.source, credentialGroup: item.credentialGroup, host: item.host, reason});
    }
  }
  return reasons;
}

export function buildView(state, nowMs = Date.now()) {
  const byStatus = {};
  for (const status of ['queued', 'leased', 'waiting_retry', 'paused', ...TERMINAL_STATUSES]) {
    byStatus[status] = 0;
  }
  const bySource = {};
  const byGroup = {};
  const byHost = {};
  const tasks = Object.values(state.tasks).map(item => ({
    id: item.id, source: item.source, station: item.station, kind: item.kind,
    from: item.from, until: item.until, priority: item.priority,
    status: item.status, attempts: item.attempts, fencing: item.fencing, pages: item.pages,
    host: item.host, credentialGroup: item.credentialGroup,
    notBefore: item.notBefore, lease: item.lease ? {...item.lease} : null,
    lastError: item.lastError ? {...item.lastError} : null,
    batchId: item.batchId, estimatedCost: item.estimatedCost,
    enqueuedAt: item.enqueuedAt, endedAt: item.endedAt,
  }));
  for (const item of tasks) {
    byStatus[item.status] += 1;
    const source = bySource[item.source] ?? {queued: 0, leased: 0, waiting_retry: 0, paused: 0,
      completed: 0, permanent_failed: 0, exhausted: 0};
    source[item.status] += 1;
    bySource[item.source] = source;
    const group = byGroup[item.credentialGroup] ?? {tasks: 0, leased: 0};
    group.tasks += 1;
    group.leased += Number(item.status === 'leased');
    byGroup[item.credentialGroup] = group;
    const host = byHost[item.host] ?? {tasks: 0, leased: 0};
    host.tasks += 1;
    host.leased += Number(item.status === 'leased');
    byHost[item.host] = host;
  }
  const next = state.config ? selectNextTasks(state, nowMs, 8) : [];
  return {
    sequence: state.sequence,
    configured: Boolean(state.config),
    config: state.config ? {...state.config} : null,
    counters: {...state.counters},
    tasks: tasks.sort((a, b) => a.id.localeCompare(b.id, 'en')),
    byStatus,
    bySource,
    byCredentialGroup: byGroup,
    byHost,
    budget: {...state.budget, available: state.budget.limit - state.budget.spent - state.budget.reserved},
    sources: Object.fromEntries(Object.entries(state.sources).map(([id, value]) => [id, {...value}])),
    nextTaskIds: next,
    blocked: describeBlocked(state, nowMs),
    staleRejections: state.rejected.slice(-20).map(item => ({...item})),
  };
}
