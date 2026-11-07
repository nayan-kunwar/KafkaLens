import { rmSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

// Resolve from the script location so stale artifacts are removed regardless
// of the caller's current working directory.
const root = path.dirname(path.dirname(fileURLToPath(import.meta.url)));

for (const target of process.argv.slice(2)) {
  rmSync(path.join(root, target), { recursive: true, force: true });
}
