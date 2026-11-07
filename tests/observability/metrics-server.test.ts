import type { AddressInfo } from 'node:net';

import { pino } from 'pino';
import { describe, expect, it } from 'vitest';

import { startMetricsServer } from '../../src/observability/metrics-server.js';
import type { Metrics } from '../../src/observability/metrics.js';

const logger = pino({ level: 'silent' });

function metrics(enabled: boolean, body = 'mcp_tool_calls_total 1\n'): Metrics {
  return {
    enabled,
    contentType: 'text/plain; version=0.0.4; charset=utf-8',
    toolCall: () => undefined,
    kafkaRequest: () => undefined,
    diagnosis: () => undefined,
    lagObservations: () => undefined,
    render: () => Promise.resolve(body),
  };
}

function portOf(server: { address(): AddressInfo | string | null }): number {
  const address = server.address();
  if (address === null || typeof address === 'string') {
    throw new Error('metrics server did not expose a TCP address');
  }
  return address.port;
}

describe('startMetricsServer', () => {
  it('does not listen when metrics are disabled', async () => {
    await expect(startMetricsServer(0, metrics(false), logger)).resolves.toBeNull();
  });

  it('serves /metrics and 404s other paths', async () => {
    const server = await startMetricsServer(0, metrics(true), logger);
    expect(server).not.toBeNull();
    try {
      const base = `http://127.0.0.1:${portOf(server as NonNullable<typeof server>)}`;
      const response = await fetch(`${base}/metrics?ignored=1`);
      expect(response.status).toBe(200);
      expect(response.headers.get('content-type')).toContain('text/plain');
      expect(await response.text()).toContain('mcp_tool_calls_total');

      const missing = await fetch(`${base}/not-metrics`);
      expect(missing.status).toBe(404);
    } finally {
      await new Promise<void>((resolve) => {
        server?.close(() => resolve());
      });
    }
  });

  it('returns 500 when rendering fails', async () => {
    const failing: Metrics = {
      ...metrics(true),
      render: () => Promise.reject(new Error('render failed')),
    };
    const server = await startMetricsServer(0, failing, logger);
    try {
      const base = `http://127.0.0.1:${portOf(server as NonNullable<typeof server>)}`;
      const response = await fetch(`${base}/metrics`);
      expect(response.status).toBe(500);
    } finally {
      await new Promise<void>((resolve) => {
        server?.close(() => resolve());
      });
    }
  });
});
