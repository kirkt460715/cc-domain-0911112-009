import {promises as fs} from 'node:fs';
import path from 'node:path';
import {randomUUID} from 'node:crypto';
import {setTimeout as delay} from 'node:timers/promises';
import {fail} from './errors.js';
import {jsonParse} from './value.js';

export async function directory(root) {
  await fs.mkdir(root, {recursive: true, mode: 0o700});
  const stat = await fs.lstat(root);
  if (!stat.isDirectory() || stat.isSymbolicLink()) {
    fail('UNSAFE_PATH', 'Store root must be a real directory');
  }
  return path.resolve(root);
}
export async function syncDirectory(root) {
  const handle = await fs.open(root, 'r');
  try {
    await handle.sync();
  } finally {
    await handle.close();
  }
}
export async function atomicWrite(file, value) {
  const temporary = `${file}.${randomUUID()}.tmp`;
  let handle;
  try {
    handle = await fs.open(temporary, 'wx', 0o600);
    await handle.writeFile(value, 'utf8');
    await handle.sync();
    await handle.close();
    handle = null;
    await fs.rename(temporary, file);
    await syncDirectory(path.dirname(file));
  } catch (error) {
    if (handle) {
      await handle.close();
    }
    await fs.rm(temporary, {force: true});
    throw error;
  }
}
export async function readOptional(file) {
  try {
    const stat = await fs.lstat(file);
    if (!stat.isFile() || stat.isSymbolicLink()) {
      fail('UNSAFE_PATH', 'Expected a regular store file');
    }
    return await fs.readFile(file, 'utf8');
  } catch (error) {
    if (error.code === 'ENOENT') {
      return null;
    }
    throw error;
  }
}
export async function readJson(file) {
  const raw = await readOptional(file);
  return raw === null ? null : jsonParse(raw, file);
}
export async function withLock(root, action, options = {}) {
  const lock = path.join(root, 'writer.lock');
  const deadline = Date.now() + (options.timeout ?? 5000);
  const token = randomUUID();
  let handle;
  while (!handle) {
    try {
      handle = await fs.open(lock, 'wx', 0o600);
    } catch (error) {
      if (error.code !== 'EEXIST') {
        throw error;
      }
      if (Date.now() >= deadline) {
        fail('LOCK_BUSY', 'Store is locked; inspect owner before recovery', {}, 409);
      }
      await delay(15);
    }
  }
  try {
    await handle.writeFile(JSON.stringify({pid: process.pid, token, created: new Date().toISOString()}));
    await handle.sync();
    return await action();
  } finally {
    await handle.close();
    const current = await readJson(lock);
    if (current?.token === token) {
      await fs.unlink(lock);
    }
  }
}
export async function recoverDeadLock(root) {
  const file = path.join(root, 'writer.lock');
  const lock = await readJson(file);
  if (lock === null) {
    return {recovered: false};
  }
  if (!Number.isInteger(lock.pid) || lock.pid <= 0 || typeof lock.token !== 'string') {
    fail('LOCK_CORRUPT', 'Cannot recover an unidentifiable lock');
  }
  try {
    process.kill(lock.pid, 0);
  } catch (error) {
    if (error.code !== 'ESRCH') {
      fail('LOCK_UNKNOWN', 'Cannot prove the lock owner is dead');
    }
    const second = await readJson(file);
    if (second?.token !== lock.token) {
      fail('LOCK_CHANGED', 'Lock changed during recovery');
    }
    await fs.unlink(file);
    return {recovered: true, owner: lock.pid};
  }
  fail('LOCK_LIVE', 'Refusing to remove a live process lock', {pid: lock.pid}, 409);
}
export async function localFile(root, relative) {
  if (typeof relative !== 'string' || path.isAbsolute(relative)) {
    fail('UNSAFE_PATH', 'A relative file path is required');
  }
  const resolved = path.resolve(root, relative);
  const prefix = `${path.resolve(root)}${path.sep}`;
  if (!resolved.startsWith(prefix)) {
    fail('UNSAFE_PATH', 'Path escapes root');
  }
  const realRoot = await fs.realpath(root);
  const realFile = await fs.realpath(resolved);
  if (!realFile.startsWith(`${realRoot}${path.sep}`)) {
    fail('UNSAFE_PATH', 'Link escapes root');
  }
  return realFile;
}
