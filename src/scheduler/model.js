import {choice, exactKeys, identifier, integer, text, timestamp} from '../value.js';
import {fail, requireValue} from '../errors.js';

export const KINDS = Object.freeze(['realtime', 'backfill']);
export const STATUSES = Object.freeze([
  'queued', 'leased', 'waiting_retry', 'paused', 'completed', 'permanent_failed', 'exhausted',
]);
export const TERMINAL_STATUSES = Object.freeze(['completed', 'permanent_failed', 'exhausted']);
export const OPEN_STATUSES = Object.freeze(['queued', 'leased', 'waiting_retry', 'paused']);

const METHODS = ['GET', 'POST'];
const RESERVED_HEADERS = ['host', 'authorization', 'content-length', 'connection'];

export function hostOf(endpoint) {
  requireValue(typeof endpoint === 'string' && endpoint.length <= 2048, 'ENDPOINT', 'Endpoint must be a URL string');
  let url;
  try {
    url = new URL(endpoint);
  } catch {
    fail('ENDPOINT', 'Endpoint is not a valid URL');
  }
  requireValue(['http:', 'https:'].includes(url.protocol), 'ENDPOINT', 'Endpoint must use http or https');
  return url.host;
}

export function normalizeTask(input) {
  exactKeys(input, [
    'id', 'source', 'station', 'kind', 'from', 'until', 'priority', 'estimatedCost',
    'endpoint', 'method', 'query', 'headers', 'credentialGroup', 'body',
  ], ['id', 'source', 'station', 'from', 'until', 'endpoint']);
  const id = identifier(input.id, 'task id');
  const source = identifier(input.source, 'source');
  const station = identifier(input.station, 'station');
  const kind = choice(input.kind ?? 'realtime', KINDS, 'kind');
  const from = timestamp(input.from, 'from');
  const until = timestamp(input.until, 'until');
  requireValue(Date.parse(until) > Date.parse(from), 'TIME_WINDOW', 'Task window end must follow start');
  const priority = integer(input.priority ?? 100, 'priority', 0, 10000);
  const estimatedCost = integer(input.estimatedCost ?? 1, 'estimatedCost', 1, 1000000);
  const endpoint = text(input.endpoint, 'endpoint', 2048);
  const host = hostOf(endpoint);
  const method = choice(input.method ?? 'GET', METHODS, 'method');
  const credentialGroup = identifier(input.credentialGroup ?? 'default', 'credentialGroup');
  const query = {};
  if (input.query !== undefined) {
    requireValue(input.query !== null && typeof input.query === 'object' && !Array.isArray(input.query),
      'INVALID_OBJECT', 'query must be an object');
    for (const [key, value] of Object.entries(input.query)) {
      requireValue(/^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$/.test(key), 'QUERY_KEY', 'Query parameter name is invalid', {key});
      requireValue(value === null || typeof value === 'string' || typeof value === 'number' || typeof value === 'boolean',
        'QUERY_VALUE', 'Query parameter values must be scalar');
      query[key] = value === null ? '' : String(value);
    }
  }
  const headers = {};
  if (input.headers !== undefined) {
    requireValue(input.headers !== null && typeof input.headers === 'object' && !Array.isArray(input.headers),
      'INVALID_OBJECT', 'headers must be an object');
    for (const [key, value] of Object.entries(input.headers)) {
      const name = String(key).toLowerCase();
      requireValue(/^[a-z0-9!#$%&'*+.^_`|~-]+$/.test(name), 'HEADER', 'Header name is invalid', {key});
      requireValue(!RESERVED_HEADERS.includes(name), 'HEADER_RESERVED', 'Header is managed by the scheduler', {header: name});
      requireValue(typeof value === 'string' && value.length <= 4096, 'HEADER', 'Header value must be text', {header: name});
      headers[name] = value;
    }
  }
  requireValue(!('x-credential-group' in headers), 'HEADER_RESERVED',
    'Credential group is sent through its own field');
  if (input.body !== undefined && input.body !== null) {
    requireValue(method === 'POST', 'TASK_BODY', 'Only POST tasks can carry a body');
    requireValue(typeof input.body === 'string' && input.body.length <= 1024 * 1024,
      'TASK_BODY', 'Body must be text no larger than 1 MiB');
  }
  return {
    id, source, station, kind, from, until, priority, estimatedCost,
    endpoint, host, method, query, headers, credentialGroup,
    body: input.body ?? null,
  };
}

export function normalizeConfig(input = {}) {
  exactKeys(input, [
    'maxConcurrency', 'hostConcurrency', 'groupConcurrency', 'sourceConcurrency',
    'hostRatePerSecond', 'groupRatePerSecond', 'budget',
    'maxAttempts', 'baseBackoffMs', 'maxBackoffMs', 'requestTimeoutMs',
    'leaseMs', 'tickMs', 'jitterMs',
  ]);
  const config = {
    maxConcurrency: integer(input.maxConcurrency ?? 8, 'maxConcurrency', 1, 256),
    hostConcurrency: integer(input.hostConcurrency ?? 4, 'hostConcurrency', 1, 256),
    groupConcurrency: integer(input.groupConcurrency ?? 2, 'groupConcurrency', 1, 256),
    sourceConcurrency: integer(input.sourceConcurrency ?? 1, 'sourceConcurrency', 1, 256),
    hostRatePerSecond: input.hostRatePerSecond == null ? null :
      integer(input.hostRatePerSecond, 'hostRatePerSecond', 1, 100000),
    groupRatePerSecond: input.groupRatePerSecond == null ? null :
      integer(input.groupRatePerSecond, 'groupRatePerSecond', 1, 100000),
    budget: integer(input.budget ?? 100000, 'budget', 1, 1000000000),
    maxAttempts: integer(input.maxAttempts ?? 6, 'maxAttempts', 1, 50),
    baseBackoffMs: integer(input.baseBackoffMs ?? 500, 'baseBackoffMs', 1, 3600000),
    maxBackoffMs: integer(input.maxBackoffMs ?? 60000, 'maxBackoffMs', 1, 86400000),
    requestTimeoutMs: integer(input.requestTimeoutMs ?? 15000, 'requestTimeoutMs', 100, 600000),
    leaseMs: integer(input.leaseMs ?? 60000, 'leaseMs', 50, 86400000),
    tickMs: integer(input.tickMs ?? 100, 'tickMs', 5, 3600000),
    jitterMs: integer(input.jitterMs ?? 100, 'jitterMs', 0, 3600000),
  };
  requireValue(config.maxBackoffMs >= config.baseBackoffMs, 'CONFIG', 'maxBackoffMs must be at least baseBackoffMs');
  return config;
}

export function parseRetryAfter(value, nowMs) {
  if (value === undefined || value === null || String(value).trim() === '') {
    return null;
  }
  const raw = String(value).trim();
  if (/^[0-9]+$/.test(raw)) {
    const seconds = Number(raw);
    if (!Number.isFinite(seconds) || seconds > 86400) {
      return null;
    }
    return seconds * 1000;
  }
  const dateMs = Date.parse(raw);
  if (!Number.isFinite(dateMs)) {
    return null;
  }
  return Math.max(0, dateMs - nowMs);
}

export function backoffDelay(attempt, config, retryAfterMs = null, random = Math.random) {
  if (retryAfterMs !== null && retryAfterMs > 0) {
    return Math.min(retryAfterMs, config.maxBackoffMs);
  }
  const exponential = Math.min(config.baseBackoffMs * 2 ** Math.max(0, attempt - 1), config.maxBackoffMs);
  const jitter = config.jitterMs ? Math.floor(random() * config.jitterMs) : 0;
  return Math.min(exponential + jitter, config.maxBackoffMs);
}
