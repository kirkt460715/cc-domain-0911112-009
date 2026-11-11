import {readdir} from 'node:fs/promises';
import {spawnSync} from 'node:child_process';
import path from 'node:path';
const files = [];
async function visit(root) {
  for (const item of await readdir(root, {withFileTypes: true})) {
    const location = path.join(root, item.name);
    if (item.isDirectory()) {
      await visit(location);
    } else if (item.name.endsWith('.js')) {
      files.push(location);
    }
  }
}
await visit('src');
await visit('tests');
for (const file of files) {
  const result = spawnSync(process.execPath, ['--check', file], {encoding: 'utf8'});
  if (result.status !== 0) {
    process.stderr.write(result.stderr);
    process.exit(result.status ?? 1);
  }
}
process.stdout.write(JSON.stringify({syntaxChecked: files.length}) + String.fromCharCode(10));
