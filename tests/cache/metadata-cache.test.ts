import { pino } from 'pino';
import { describe, expect, it } from 'vitest';

import { MetadataCache } from '../../src/cache/metadata-cache.js';

const logger = pino({ level: 'silent' });

describe('MetadataCache', () => {
  it('continues without a cache when Redis cannot connect', async () => {
    const cache = await MetadataCache.connect('redis://localhost:1', logger, () => {
      throw new Error('connection refused');
    });
    expect(await cache.get('cluster-info')).toBeNull();
    await cache.set('cluster-info', '{}', 15);
    await cache.close();
  });

  it('reads and writes through a connected client', async () => {
    const values = new Map<string, string>();
    const cache = await MetadataCache.connect('redis://localhost:6379', logger, () => ({
      isOpen: true,
      connect: () => Promise.resolve(),
      quit: () => Promise.resolve(),
      get: (key: string) => Promise.resolve(values.get(key) ?? null),
      set: (key: string, value: string) => {
        values.set(key, value);
        return Promise.resolve('OK');
      },
      on: () => undefined,
    }));
    await cache.set('cluster-info', '{"clusterId":"c"}', 15);
    expect(await cache.get('cluster-info')).toBe('{"clusterId":"c"}');
    expect([...values.keys()][0]).toContain('kafka-lens:v1:');
    await cache.close();
  });
});
