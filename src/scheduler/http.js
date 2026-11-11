import {LedgerError, fail} from '../errors.js';
import {identifier} from '../value.js';

export const SCHEDULER_ROUTES = Object.freeze({
  GET: new Set(['/scheduler/status']),
  POST: new Set([
    '/scheduler/tasks',
    '/scheduler/sources/pause',
    '/scheduler/sources/resume',
    '/scheduler/tasks/takeover',
  ]),
});

export function isSchedulerRoute(method, route) {
  return SCHEDULER_ROUTES[method]?.has(route) ?? false;
}

export async function dispatchSchedulerRoute(engine, method, route, input) {
  if (!engine) {
    fail('NOT_FOUND', 'Endpoint does not exist', {}, 404);
  }
  switch (route) {
    case '/scheduler/status':
      return engine.view();
    case '/scheduler/tasks':
      return engine.enqueue(input);
    case '/scheduler/sources/pause':
      identifier(input?.source, 'source');
      return engine.pauseSource(input.source);
    case '/scheduler/sources/resume':
      identifier(input?.source, 'source');
      return engine.resumeSource(input.source);
    case '/scheduler/tasks/takeover':
      identifier(input?.id, 'task id');
      return engine.takeOver(input.id);
    default:
      fail('NOT_FOUND', 'Endpoint does not exist', {}, 404);
  }
}

export async function schedulerRequest(engine, method, route, input) {
  try {
    return {status: 200, body: await dispatchSchedulerRoute(engine, method, route, input)};
  } catch (error) {
    if (error instanceof LedgerError) {
      return {status: error.status, body: {error: error.code, message: error.message, details: error.details}};
    }
    throw error;
  }
}
