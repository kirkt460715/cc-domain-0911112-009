import {requireValue} from '../errors.js';
import {parseRetryAfter} from './model.js';

export const OUTCOMES = Object.freeze({
  SUCCESS: 'success',
  RATE_LIMITED: 'rate_limited',
  RETRYABLE: 'retryable',
  PERMANENT: 'permanent',
});

const RETRYABLE_STATUSES = new Set([408, 409, 425, 500, 502, 503, 504]);

export function buildRequest(task) {
  const url = new URL(task.endpoint);
  for (const [key, value] of Object.entries(task.query ?? {})) {
    url.searchParams.set(key, value);
  }
  if (task.cursor !== null) {
    url.searchParams.set('cursor', task.cursor);
  }
  const headers = {...task.headers, 'x-credential-group': task.credentialGroup};
  const init = {method: task.method, headers};
  if (task.method === 'POST') {
    headers['content-type'] = headers['content-type'] ?? 'application/json';
    if (task.body !== null) {
      init.body = task.body;
    }
  }
  return {url: url.toString(), init};
}

export function classifyResponse(status) {
  if (status >= 200 && status < 300) {
    return OUTCOMES.SUCCESS;
  }
  if (status === 429) {
    return OUTCOMES.RATE_LIMITED;
  }
  if (RETRYABLE_STATUSES.has(status)) {
    return OUTCOMES.RETRYABLE;
  }
  return OUTCOMES.PERMANENT;
}

export async function callGateway(task, {timeoutMs, fetchImpl = fetch, now = () => Date.now()} = {}) {
  requireValue(Number.isSafeInteger(timeoutMs) && timeoutMs > 0, 'CONFIG', 'requestTimeoutMs is required');
  const {url, init} = buildRequest(task);
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(new Error('scheduler request timeout')), timeoutMs);
  let response;
  try {
    response = await fetchImpl(url, {...init, signal: controller.signal});
  } catch (error) {
    clearTimeout(timer);
    return {
      outcome: OUTCOMES.RETRYABLE,
      kind: 'network',
      status: null,
      retryAfterMs: null,
      payload: null,
      message: error?.name === 'AbortError' ? 'request timed out' : (error?.message ?? 'network failure'),
    };
  }
  clearTimeout(timer);
  const status = response.status;
  const kind = classifyResponse(status);
  const retryAfterMs = kind === OUTCOMES.RATE_LIMITED || kind === OUTCOMES.RETRYABLE ?
    parseRetryAfter(response.headers.get('retry-after'), now()) : null;
  let payload = null;
  const text = await response.text();
  if (text) {
    try {
      payload = JSON.parse(text);
    } catch {
      payload = null;
    }
  }
  return {
    outcome: kind,
    kind: kind === OUTCOMES.RATE_LIMITED ? 'rate_limited' : (kind === OUTCOMES.PERMANENT ? 'permanent' : 'http'),
    status,
    retryAfterMs,
    payload,
    message: kind === OUTCOMES.SUCCESS ? null : `gateway responded ${status}`,
  };
}

export function parseGatewayPayload(payload) {
  requireValue(payload !== null && typeof payload === 'object' && !Array.isArray(payload),
    'GATEWAY_PAYLOAD', 'Gateway payload must be a JSON object');
  requireValue(Array.isArray(payload.records), 'GATEWAY_PAYLOAD', 'Gateway payload requires a records array');
  const complete = payload.complete !== false;
  const cursor = payload.cursor === undefined || payload.cursor === null ? null : String(payload.cursor);
  requireValue(complete || cursor !== null, 'GATEWAY_PAYLOAD',
    'Incomplete gateway page must provide the next cursor');
  return {records: payload.records, complete, cursor};
}
