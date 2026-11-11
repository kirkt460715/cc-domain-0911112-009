import {promises as fs} from 'node:fs';
import path from 'node:path';
import {Store} from './store.js';
import {atomicWrite, readJson, readOptional, syncDirectory} from './files.js';
import {digest, exactKeys, integer} from './value.js';
import {requireValue} from './errors.js';

export async function inspectBackup(root) {
  const manifest = await readJson(path.join(root, 'backup.json'));
  requireValue(manifest, 'BACKUP_MANIFEST', 'Backup manifest is missing');
  exactKeys(manifest, ['format', 'sequence', 'head', 'digest'], ['format', 'sequence', 'head', 'digest']);
  requireValue(manifest.format === 1, 'BACKUP_FORMAT', 'Unsupported backup format');
  integer(manifest.sequence, 'sequence', 0);
  const journal = await readOptional(path.join(root, 'journal.jsonl'));
  requireValue(journal !== null, 'BACKUP_JOURNAL', 'Backup journal is missing');
  requireValue(digest(journal) === manifest.digest, 'BACKUP_CHECKSUM', 'Backup content checksum differs');
  const store = new Store(root);
  const {state} = await store.replay();
  requireValue(state.sequence === manifest.sequence && state.head === manifest.head,
    'BACKUP_HEAD', 'Backup journal does not match its declared head');
  return {manifest, journal, state};
}
export async function restoreBackup(source, destination) {
  const target = path.resolve(destination);
  requireValue(target !== path.resolve(source), 'RESTORE_PATH', 'Restore target must differ from backup');
  const inspected = await inspectBackup(source);
  let created = false;
  try {
    await fs.mkdir(target, {recursive: false, mode: 0o700});
    created = true;
    await atomicWrite(path.join(target, 'journal.jsonl'), inspected.journal);
    await syncDirectory(target);
    const restored = new Store(target);
    const checked = await restored.verify();
    requireValue(checked.head === inspected.manifest.head, 'RESTORE_VERIFY', 'Restored store head differs');
    return {restored: true, destination: target, sequence: inspected.manifest.sequence, head: checked.head};
  } catch (error) {
    if (created) {
      await fs.rm(target, {recursive: true, force: true});
    }
    throw error;
  }
}
export async function compareBackupToStore(backupRoot, storeRoot) {
  const backup = await inspectBackup(backupRoot);
  const current = await new Store(storeRoot).read();
  const result = {
    backupSequence: backup.state.sequence,
    currentSequence: current.sequence,
    equal: backup.state.head === current.head,
    observationsAdded: [],
    observationsChanged: [],
    observationsRemoved: [],
  };
  const remaining = new Set(Object.keys(backup.state.observations));
  for (const [key, item] of Object.entries(current.observations)) {
    const prior = backup.state.observations[key];
    if (!prior) {
      result.observationsAdded.push(key);
    } else if (digest(item) !== digest(prior)) {
      result.observationsChanged.push({key, beforeRevision: prior.revision, afterRevision: item.revision});
    }
    remaining.delete(key);
  }
  result.observationsRemoved = [...remaining].sort();
  result.observationsAdded.sort();
  result.observationsChanged.sort((a, b) => a.key.localeCompare(b.key));
  return result;
}
