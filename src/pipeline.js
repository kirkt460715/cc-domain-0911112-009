import {promises as fs} from 'node:fs';
import path from 'node:path';
import {localFile} from './files.js';
import {parseCsv, decodeJsonLines} from './adapters.js';
import {mapRows, cumulativeRainfall, alignIntervals} from './transforms.js';
import {array, choice, digest, exactKeys, identifier, jsonParse, timestamp} from './value.js';
import {requireValue} from './errors.js';
import {prepareBatch} from './ingest.js';
import {projectEvents} from './state.js';

export function manifestInput(input) {
  exactKeys(input, ['id', 'source', 'receivedAt', 'mode', 'files', 'mapping', 'counter', 'alignment'],
    ['id', 'source', 'receivedAt', 'files']);
  const files = array(input.files, 'files', 100);
  requireValue(files.length > 0, 'FILES', 'Manifest must name at least one file');
  const seen = new Set();
  for (const file of files) {
    exactKeys(file, ['path', 'format', 'checksum'], ['path', 'format', 'checksum']);
    requireValue(typeof file.path === 'string' && !seen.has(file.path), 'FILES', 'File path is missing or repeated');
    seen.add(file.path);
    choice(file.format, ['csv', 'jsonl', 'json'], 'format');
    requireValue(/^[a-f0-9]{64}$/.test(file.checksum), 'CHECKSUM', 'Each file requires SHA256');
  }
  return {
    id: identifier(input.id),
    source: identifier(input.source),
    receivedAt: timestamp(input.receivedAt),
    mode: choice(input.mode ?? 'atomic', ['atomic', 'partial'], 'mode'),
    files,
    mapping: input.mapping ?? null,
    counter: input.counter ?? null,
    alignment: input.alignment ?? null,
  };
}
export async function prepareImport(root, input) {
  const manifest = manifestInput(input);
  const all = [];
  const provenance = [];
  for (const file of manifest.files) {
    const location = await localFile(root, file.path);
    const stat = await fs.stat(location);
    requireValue(stat.isFile() && stat.size <= 16 * 1024 * 1024, 'FILE_SIZE', 'Import file exceeds size limit');
    const content = await fs.readFile(location, 'utf8');
    requireValue(digest(content) === file.checksum, 'FILE_CHECKSUM', 'Import file changed', {path: file.path});
    let rows;
    if (file.format === 'csv') {
      rows = parseCsv(content);
    } else if (file.format === 'jsonl') {
      rows = decodeJsonLines(content);
    } else {
      rows = array(jsonParse(content), 'file rows');
    }
    all.push(...rows);
    requireValue(all.length <= 10000, 'LIMIT', 'Combined import exceeds record limit');
    provenance.push({path: file.path, checksum: file.checksum, rows: rows.length});
  }
  let records = all;
  const diagnostics = [];
  if (manifest.mapping) {
    const mapped = mapRows(records, manifest.mapping);
    records = mapped.records;
    diagnostics.push(...mapped.skipped.map(item => ({stage: 'mapping', ...item})));
  }
  if (manifest.counter) {
    const transformed = cumulativeRainfall(records, manifest.counter);
    records = transformed.records;
    diagnostics.push(...transformed.gaps.map(item => ({stage: 'counter', ...item})));
  }
  if (manifest.alignment) {
    const aligned = alignIntervals(records, manifest.alignment);
    records = aligned.records;
    diagnostics.push(...aligned.rejected.map(item => ({stage: 'alignment', ...item})));
  }
  requireValue(records.length > 0, 'EMPTY_IMPORT', 'No observations remain after transformation');
  return {
    batch: {id: manifest.id, source: manifest.source, receivedAt: manifest.receivedAt, mode: manifest.mode, records},
    provenance,
    diagnostics,
    manifestHash: digest(manifest),
  };
}
export async function runImport(ledger, root, manifest, {preview = false, expectedSequence, actor = 'import'} = {}) {
  const prepared = await prepareImport(root, manifest);
  const result = preview ? await ledger.preview(prepared.batch) :
    await ledger.ingest(prepared.batch, actor, {expectedSequence});
  return {...prepared, result, preview};
}
export function simulateBatches(state, batches) {
  let projected = structuredClone(state);
  const results = [];
  for (const batch of array(batches, 'batches', 100)) {
    const decision = prepareBatch(projected, batch);
    projected = projectEvents(projected, decision.events);
    results.push(decision.result);
  }
  return {results, projected};
}
export async function replayImportPlan(ledger, root, input) {
  exactKeys(input, ['imports', 'actor'], ['imports']);
  const imports = array(input.imports, 'imports', 100);
  const actor = identifier(input.actor ?? 'replay');
  const results = [];
  for (const manifest of imports) {
    const result = await runImport(ledger, root, manifest, {actor});
    results.push({id: result.batch.id, ...result.result, manifestHash: result.manifestHash});
  }
  return {completed: results.length, results};
}
