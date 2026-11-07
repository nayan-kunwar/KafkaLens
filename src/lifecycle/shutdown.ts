import type { Logger } from 'pino';

export interface ShutdownOptions {
  logger: Pick<Logger, 'info' | 'error'>;
  /** Persists in-flight observations before dependent resources close. */
  flushHistory: () => Promise<void>;
  /** Closes the transport, stores, cache, and optional metrics server. */
  close: () => Promise<unknown>[];
  exit: (code: number) => void;
  timeoutMs?: number;
}

const DEFAULT_TIMEOUT_MS = 10_000;

/**
 * Builds the SIGINT/SIGTERM handler. It runs once, flushes history, closes
 * every resource with `allSettled`, and exits 0. A hung resource triggers a
 * forced exit 1 after the deadline.
 */
export function createShutdownHandler(options: ShutdownOptions): (signal: NodeJS.Signals) => void {
  const timeoutMs = options.timeoutMs ?? DEFAULT_TIMEOUT_MS;
  let shuttingDown = false;
  let exited = false;
  const finish = (code: number): void => {
    if (exited) return;
    exited = true;
    options.exit(code);
  };

  return (signal) => {
    if (shuttingDown) return;
    shuttingDown = true;
    options.logger.info({ signal }, 'shutting down');

    const force = setTimeout(() => {
      options.logger.error('shutdown timed out; forcing exit');
      finish(1);
    }, timeoutMs);
    force.unref();

    void (async () => {
      await options.flushHistory().catch(() => undefined);
      try {
        await Promise.allSettled(options.close());
      } finally {
        clearTimeout(force);
        finish(0);
      }
    })();
  };
}
