import {fail, requireValue} from './errors.js';
import {canonical, jsonParse, identifier, integer, timestamp} from './value.js';
const LF = String.fromCharCode(10);
const CR = String.fromCharCode(13);
const QUOTE = String.fromCharCode(34);

export function parseCsv(input, {maxRows = 10000, maxField = 4096} = {}) {
  requireValue(typeof input === 'string', 'CSV', 'CSV input must be text');
  const text = input.charCodeAt(0) === 0xfeff ? input.slice(1) : input;
  const records = [];
  let row = [];
  let field = '';
  let quoted = false;
  let closedQuote = false;
  let fieldStarted = false;
  function endField() {
    row.push(field);
    field = '';
    fieldStarted = false;
    closedQuote = false;
  }
  function endRow() {
    endField();
    records.push(row);
    row = [];
    requireValue(records.length <= maxRows + 1, 'LIMIT', 'CSV contains too many rows');
  }
  for (let index = 0; index < text.length; index += 1) {
    const char = text[index];
    if (quoted) {
      if (char === QUOTE) {
        if (text[index + 1] === QUOTE) {
          field += QUOTE;
          index += 1;
        } else {
          quoted = false;
          closedQuote = true;
        }
      } else {
        field += char;
      }
    } else if (char === ',') {
      endField();
    } else if (char === LF || char === CR) {
      if (char === CR && text[index + 1] === LF) {
        index += 1;
      }
      endRow();
    } else if (char === QUOTE && !fieldStarted && !closedQuote) {
      quoted = true;
      fieldStarted = true;
    } else {
      requireValue(!closedQuote && char !== QUOTE, 'CSV_QUOTE', 'Unexpected character after closing quote');
      field += char;
      fieldStarted = true;
    }
    requireValue(field.length <= maxField, 'LIMIT', 'CSV field exceeds maximum length');
  }
  requireValue(!quoted, 'CSV_QUOTE', 'Unclosed CSV quote');
  if (fieldStarted || field.length || row.length || closedQuote) {
    endRow();
  }
  if (!records.length) {
    fail('CSV_EMPTY', 'CSV has no header');
  }
  const header = records.shift();
  requireValue(header.every(name => name.length > 0), 'CSV_HEADER', 'CSV has an empty column name');
  requireValue(new Set(header).size === header.length, 'CSV_HEADER', 'CSV has duplicate column names');
  for (const name of header) {
    identifier(name, 'column');
  }
  return records.map((values, index) => {
    requireValue(values.length === header.length, 'CSV_WIDTH', 'CSV row has wrong column count', {row: index + 2});
    return Object.fromEntries(header.map((name, column) => [name, values[column]]));
  });
}
export function encodeCsv(rows, columns) {
  function cell(value) {
    const raw = value === null || value === undefined ? '' : String(value);
    const safe = /^[=+@-]/.test(raw) && typeof value === 'string' ? `'${raw}` : raw;
    if (safe.includes(',') || safe.includes(QUOTE) || safe.includes(LF) || safe.includes(CR)) {
      return QUOTE + safe.split(QUOTE).join(QUOTE + QUOTE) + QUOTE;
    }
    return safe;
  }
  const lines = [columns.map(cell).join(',')];
  for (const row of rows) {
    lines.push(columns.map(column => cell(row[column])).join(','));
  }
  return lines.join(LF) + LF;
}
export function decodeObservationCsv(input) {
  const rows = parseCsv(input);
  return rows.map((row, index) => {
    const allowed = ['station', 'metric', 'time', 'revision', 'operation', 'value', 'unit', 'quality'];
    for (const key of Object.keys(row)) {
      requireValue(allowed.includes(key), 'CSV_HEADER', 'Unexpected observation column', {key});
    }
    const revision = Number(row.revision);
    integer(revision, 'revision', 1);
    const result = {...row, revision};
    if (!result.operation) {
      delete result.operation;
    }
    if (!result.quality) {
      delete result.quality;
    }
    if (result.operation === 'delete') {
      requireValue(!row.value && !row.unit, 'DELETE_VALUE', 'CSV deletion has measurement fields', {index});
      delete result.value;
      delete result.unit;
    } else {
      requireValue(row.value !== undefined && row.value.trim() !== '', 'CSV_NUMBER', 'CSV value is empty', {index});
      result.value = Number(row.value);
      requireValue(Number.isFinite(result.value), 'CSV_NUMBER', 'CSV value is not finite', {index});
    }
    return result;
  });
}
export function decodeJsonLines(input, limit = 10000) {
  const lines = input.split(LF);
  const rows = [];
  for (let index = 0; index < lines.length; index += 1) {
    if (!lines[index].trim()) {
      continue;
    }
    requireValue(rows.length < limit, 'LIMIT', 'Too many JSONL records');
    rows.push(jsonParse(lines[index], `line ${index + 1}`));
  }
  return rows;
}
export function encodeJsonLines(rows) {
  return rows.map(row => canonical(row)).join(LF) + (rows.length ? LF : '');
}
export function importEnvelope(content, format, header) {
  const id = identifier(header.id);
  const source = identifier(header.source);
  const receivedAt = timestamp(header.receivedAt);
  let records;
  if (format === 'csv') {
    records = decodeObservationCsv(content);
  } else if (format === 'jsonl') {
    records = decodeJsonLines(content);
  } else if (format === 'json') {
    records = jsonParse(content);
  } else {
    fail('FORMAT', 'Supported import formats are csv, jsonl and json');
  }
  return {id, source, receivedAt, mode: header.mode ?? 'partial', records};
}
