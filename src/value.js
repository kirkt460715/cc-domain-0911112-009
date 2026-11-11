import {createHash, timingSafeEqual} from 'node:crypto';
import {fail, requireValue} from './errors.js';

export function object(value, label) {
  requireValue(value !== null && typeof value === 'object' && !Array.isArray(value),
    'INVALID_OBJECT', `${label} must be an object`);
  const prototype = Object.getPrototypeOf(value);
  requireValue(prototype === Object.prototype || prototype === null,
    'INVALID_OBJECT', `${label} must be a plain object`);
  return value;
}
export function text(value, label, maximum = 256) {
  requireValue(typeof value === 'string', 'INVALID_TEXT', `${label} must be text`);
  const result = value.normalize('NFC').trim();
  requireValue(result.length > 0 && result.length <= maximum,
    'INVALID_TEXT', `${label} length is invalid`);
  requireValue(!Array.from(result).some(c => c.charCodeAt(0) < 32),
    'INVALID_TEXT', `${label} contains control characters`);
  return result;
}
export function identifier(value, label = 'id') {
  const result = text(value, label, 96);
  requireValue(/^[A-Za-z0-9][A-Za-z0-9_.:-]*$/.test(result),
    'INVALID_ID', `${label} has invalid characters`);
  requireValue(!['__proto__', 'prototype', 'constructor'].includes(result),
    'INVALID_ID', `${label} is reserved`);
  return result;
}
export function integer(value, label, minimum = 0, maximum = Number.MAX_SAFE_INTEGER) {
  requireValue(Number.isSafeInteger(value) && value >= minimum && value <= maximum,
    'INVALID_INTEGER', `${label} is outside its allowed range`);
  return value;
}
export function number(value, label, minimum = -Infinity, maximum = Infinity) {
  requireValue(typeof value === 'number' && Number.isFinite(value),
    'INVALID_NUMBER', `${label} must be finite`);
  requireValue(value >= minimum && value <= maximum,
    'INVALID_NUMBER', `${label} is outside its allowed range`);
  return Object.is(value, -0) ? 0 : value;
}
export function choice(value, choices, label) {
  requireValue(choices.includes(value), 'INVALID_CHOICE', `${label} is unsupported`, {choices});
  return value;
}
export function array(value, label, maximum = 10000) {
  requireValue(Array.isArray(value), 'INVALID_ARRAY', `${label} must be an array`);
  requireValue(value.length <= maximum, 'LIMIT', `${label} is too large`, {maximum});
  return value;
}
export function exactKeys(value, allowed, required = []) {
  object(value, 'input');
  for (const key of Object.keys(value)) {
    requireValue(allowed.includes(key), 'UNKNOWN_FIELD', `Unknown field: ${key}`);
  }
  for (const key of required) {
    requireValue(Object.hasOwn(value, key), 'MISSING_FIELD', `Missing field: ${key}`);
  }
}
export function timestamp(value, label = 'timestamp') {
  const input = text(value, label, 40);
  requireValue(/^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:[.][0-9]{1,3})?(?:Z|[+-][0-9]{2}:[0-9]{2})$/.test(input),
    'INVALID_TIME', `${label} needs an explicit timezone`);
  const epoch = Date.parse(input);
  requireValue(Number.isFinite(epoch), 'INVALID_TIME', `${label} cannot be parsed`);
  const local = input.slice(0, 19);
  const [y, m, d, h, minute, second] = local.match(/[0-9]+/g).map(Number);
  const days = new Date(Date.UTC(y, m, 0)).getUTCDate();
  requireValue(m >= 1 && m <= 12 && d >= 1 && d <= days && h <= 23 && minute <= 59 && second <= 59,
    'INVALID_TIME', `${label} is not a calendar time`);
  return new Date(epoch).toISOString();
}
export function canonical(value) {
  if (value === null || typeof value === 'boolean' || typeof value === 'string') {
    return JSON.stringify(value);
  }
  if (typeof value === 'number') {
    number(value, 'JSON number');
    return JSON.stringify(value);
  }
  if (Array.isArray(value)) {
    return `[${value.map(canonical).join(',')}]`;
  }
  object(value, 'JSON value');
  return `{${Object.keys(value).sort().map(key => `${JSON.stringify(key)}:${canonical(value[key])}`).join(',')}}`;
}
export function digest(value) {
  return createHash('sha256').update(typeof value === 'string' ? value : canonical(value)).digest('hex');
}
export function clone(value) {
  return structuredClone(value);
}
export function same(a, b) {
  return canonical(a) === canonical(b);
}
export function unique(values, label) {
  requireValue(new Set(values).size === values.length, 'DUPLICATE', `${label} contains duplicates`);
  return values;
}
export function jsonParse(input, label = 'JSON') {
  try {
    const result = JSON.parse(input);
    canonical(result);
    return result;
  } catch (error) {
    fail('INVALID_JSON', `${label} cannot be decoded`, {cause: error.message});
  }
}
export function positiveDuration(value, label, maximum = 86400000) {
  return integer(value, label, 1, maximum);
}
export function optionalText(value, label, maximum) {
  return value === undefined || value === null ? null : text(value, label, maximum);
}
export function secureEqual(a, b) {
  if (typeof a !== 'string' || typeof b !== 'string') {
    return false;
  }
  const left = Buffer.from(a);
  const right = Buffer.from(b);
  return left.length === right.length && timingSafeEqual(left, right);
}
export function orderedEntries(record) {
  return Object.entries(record).sort(([a], [b]) => a.localeCompare(b, 'en'));
}
export function assertCount(count, limit, label) {
  if (count > limit) {
    fail('LIMIT', `${label} exceeds limit`, {count, limit}, 413);
  }
}
