import { mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';

import { afterEach, describe, expect, it } from 'vitest';

import { defaultEnvCandidates, loadDotenv } from '../../src/config/load-dotenv.js';

const keys = ['TEST_DOTENV_A', 'TEST_DOTENV_B', 'TEST_DOTENV_PRE'] as const;
const tempDirs: string[] = [];

function tempEnvFile(content: string): string {
  const dir = mkdtempSync(path.join(tmpdir(), 'kafka-lens-dotenv-'));
  tempDirs.push(dir);
  const file = path.join(dir, '.env');
  writeFileSync(file, content);
  return file;
}

afterEach(() => {
  for (const key of keys) {
    delete process.env[key];
  }
  while (tempDirs.length > 0) {
    const dir = tempDirs.pop();
    if (dir !== undefined) {
      rmSync(dir, { recursive: true, force: true });
    }
  }
});

describe('loadDotenv', () => {
  it('loads values into process.env', () => {
    const file = tempEnvFile('TEST_DOTENV_A=from-file\nTEST_DOTENV_B=second\n');

    const loaded = loadDotenv([file]);

    expect(loaded).toEqual([file]);
    expect(process.env.TEST_DOTENV_A).toBe('from-file');
    expect(process.env.TEST_DOTENV_B).toBe('second');
  });

  it('does not override variables already set in the environment', () => {
    process.env.TEST_DOTENV_PRE = 'from-env';
    const file = tempEnvFile('TEST_DOTENV_PRE=from-file\nTEST_DOTENV_A=new\n');

    loadDotenv([file]);

    expect(process.env.TEST_DOTENV_PRE).toBe('from-env');
    expect(process.env.TEST_DOTENV_A).toBe('new');
  });

  it('gives earlier candidates precedence over later ones', () => {
    const first = tempEnvFile('TEST_DOTENV_A=from-first\n');
    const second = tempEnvFile('TEST_DOTENV_A=from-second\nTEST_DOTENV_B=from-second\n');

    loadDotenv([first, second]);

    expect(process.env.TEST_DOTENV_A).toBe('from-first');
    expect(process.env.TEST_DOTENV_B).toBe('from-second');
  });

  it('is a no-op when no candidate file exists', () => {
    const missing = path.join(tmpdir(), 'kafka-lens-dotenv-missing', '.env');

    const loaded = loadDotenv([missing]);

    expect(loaded).toEqual([]);
    expect(process.env.TEST_DOTENV_A).toBeUndefined();
  });

  it('ignores duplicate candidate paths', () => {
    const file = tempEnvFile('TEST_DOTENV_A=only-once\n');

    const loaded = loadDotenv([file, file]);

    expect(loaded).toEqual([file]);
    expect(process.env.TEST_DOTENV_A).toBe('only-once');
  });
});

describe('defaultEnvCandidates', () => {
  it('returns absolute .env paths rooted at the project and the cwd', () => {
    const candidates = defaultEnvCandidates();

    expect(candidates.length).toBeGreaterThanOrEqual(1);
    for (const candidate of candidates) {
      expect(path.isAbsolute(candidate)).toBe(true);
      expect(path.basename(candidate)).toBe('.env');
    }
    expect(candidates[0]).toBe(path.resolve(process.cwd(), '.env'));
  });
});
