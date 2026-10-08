import { existsSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

import { config } from 'dotenv';

const PROJECT_ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../..');

export function defaultEnvCandidates(): string[] {
  return [path.join(PROJECT_ROOT, '.env'), path.join(process.cwd(), '.env')];
}

export function loadDotenv(candidates: string[] = defaultEnvCandidates()): string[] {
  const files = [...new Set(candidates)].filter((file) => existsSync(file));

  if (files.length === 0) {
    return [];
  }

  const result = config({ path: files, quiet: true });

  if (result.error) {
    process.stderr.write(`dotenv: failed to load ${files.join(', ')}: ${result.error.message}\n`);
    return [];
  }

  return files;
}
