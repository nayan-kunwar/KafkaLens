import { cpSync, mkdirSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

// Resolve from the script location so the copy works regardless of cwd.
const root = path.dirname(path.dirname(fileURLToPath(import.meta.url)));

mkdirSync(path.join(root, 'dist/persistence/migrations'), { recursive: true });
cpSync(
  path.join(root, 'src/persistence/migrations'),
  path.join(root, 'dist/persistence/migrations'),
  { recursive: true },
);
