import { describe, expect, it } from 'vitest';

import { CorrelationService, redactSecrets } from '../../src/services/correlation-service.js';

const window = { since: '2026-01-01T00:00:00.000Z', until: '2026-01-01T01:00:00.000Z' };

describe('CorrelationService', () => {
  it('reports not_configured when no source is set', async () => {
    const result = await new CorrelationService().correlate(window);
    expect(result.status).toBe('not_configured');
    expect(result.events).toEqual([]);
  });

  it('maps GitHub commits and deployments inside the window', async () => {
    const fetchImpl: typeof fetch = (input) => {
      const url =
        typeof input === 'string' ? input : input instanceof URL ? input.toString() : input.url;
      const body = url.includes('/commits')
        ? [
            {
              sha: 'abcdef1234567890',
              html_url: 'https://example.test/commit/abc',
              commit: {
                message: 'ship lag fix\n\nmore',
                author: { date: '2026-01-01T00:30:00.000Z' },
              },
            },
          ]
        : [
            {
              id: 7,
              environment: 'prod',
              description: 'payment-worker',
              created_at: '2026-01-01T00:40:00.000Z',
            },
          ];
      return Promise.resolve(new Response(JSON.stringify(body), { status: 200 }));
    };
    const result = await new CorrelationService({
      githubRepository: 'acme/payments',
      fetchImpl,
    }).correlate(window);
    expect(result.status).toBe('ok');
    expect(result.events.map((event) => event.title)).toEqual(['payment-worker', 'ship lag fix']);
    expect(result.message).toMatch(/not causes/);
  });

  it('redacts a token from a failed request and keeps diagnosis data unavailable', async () => {
    const fetchImpl: typeof fetch = () =>
      Promise.reject(new Error('Bearer super-secret-token failed'));
    const result = await new CorrelationService({
      githubRepository: 'acme/payments',
      githubToken: 'super-secret-token',
      fetchImpl,
    }).correlate(window);
    expect(result.status).toBe('unavailable');
    expect(result.message).not.toContain('super-secret-token');
    expect(result.message).toContain('[redacted]');
  });

  it('reads a deployment feed', async () => {
    const fetchImpl: typeof fetch = () =>
      Promise.resolve(
        new Response(
          JSON.stringify([
            {
              id: 'd1',
              service: 'payment-worker',
              version: '1.2.3',
              deployedAt: '2026-01-01T00:10:00.000Z',
            },
          ]),
          { status: 200 },
        ),
      );
    const result = await new CorrelationService({
      deploymentSourceUrl: 'https://deploy.example.test/events',
      fetchImpl,
    }).correlate(window);
    expect(result.events).toEqual([
      {
        source: 'deployment',
        id: 'd1',
        title: 'payment-worker 1.2.3',
        occurredAt: '2026-01-01T00:10:00.000Z',
      },
    ]);
  });
});

describe('redactSecrets', () => {
  it('strips userinfo and bearer tokens', () => {
    expect(redactSecrets('postgres://user:secret@db/app Bearer abc', ['abc'])).toBe(
      'postgres://db/app Bearer [redacted]',
    );
  });
});
