import type { Logger } from 'pino';
import { createClient } from 'redis';

export interface KeyValueCache {
  get(key: string): Promise<string | null>;
  set(key: string, value: string, ttlSeconds: number): Promise<void>;
  close(): Promise<void>;
}

interface RedisLike {
  isOpen: boolean;
  connect(): Promise<unknown>;
  quit(): Promise<unknown>;
  get(key: string): Promise<string | null>;
  set(key: string, value: string, options: { EX: number }): Promise<unknown>;
  ping?(): Promise<string>;
  on(event: 'error', listener: (err: Error) => void): unknown;
}

const KEY_PREFIX = 'kafka-lens:v1:';

/**
 * Short-lived metadata cache. Redis is not a source of truth: a connection
 * failure disables the cache and callers read Kafka directly.
 */
export class MetadataCache implements KeyValueCache {
  private constructor(private readonly client: RedisLike | null) {}

  static async connect(
    url: string,
    logger: Logger,
    factory: (url: string) => RedisLike = defaultFactory,
  ): Promise<MetadataCache> {
    try {
      const client = factory(url);
      client.on('error', (err) => {
        logger.warn({ err: err.message }, 'redis cache error');
      });
      await client.connect();
      logger.info('redis metadata cache connected');
      return new MetadataCache(client);
    } catch (err) {
      logger.warn(
        { err: err instanceof Error ? err.message : String(err) },
        'redis unavailable, continuing without metadata cache',
      );
      return new MetadataCache(null);
    }
  }

  async get(key: string): Promise<string | null> {
    if (this.client === null || !this.client.isOpen) return null;
    try {
      return await this.client.get(KEY_PREFIX + key);
    } catch {
      return null;
    }
  }

  async set(key: string, value: string, ttlSeconds: number): Promise<void> {
    if (this.client === null || !this.client.isOpen || ttlSeconds <= 0) return;
    try {
      await this.client.set(KEY_PREFIX + key, value, { EX: ttlSeconds });
    } catch {
      return;
    }
  }

  async status(): Promise<'ok' | 'unavailable'> {
    if (this.client === null || !this.client.isOpen) return 'unavailable';
    // Without ping there is no round-trip to verify: report unverified, not ok.
    if (this.client.ping === undefined) return 'unavailable';
    try {
      await this.client.ping();
      return 'ok';
    } catch {
      return 'unavailable';
    }
  }

  async close(): Promise<void> {
    if (this.client?.isOpen) {
      await this.client.quit().catch(() => undefined);
    }
  }
}

function defaultFactory(url: string): RedisLike {
  return createClient({
    url,
    disableOfflineQueue: true,
    socket: {
      connectTimeout: 2000,
      reconnectStrategy: () => new Error('redis unavailable'),
    },
  });
}
