import { describe, expect, it } from 'vitest';
import { z } from 'zod';

import { getLagHistoryShape } from '../../src/schemas/history.js';

describe('getLagHistoryShape', () => {
  const schema = z.object(getLagHistoryShape);

  it('defaults limit to 100', () => {
    expect(schema.parse({ groupId: 'g', topic: 't' })).toMatchObject({ limit: 100 });
  });

  it('accepts partition, since, and a bounded limit', () => {
    expect(
      schema.parse({
        groupId: 'g',
        topic: 't',
        partition: 2,
        since: '2026-01-01T00:00:00.000Z',
        limit: 10,
      }),
    ).toMatchObject({ partition: 2, limit: 10 });
  });

  it('rejects out-of-range limits and non-ISO since values', () => {
    expect(() => schema.parse({ groupId: 'g', topic: 't', limit: 0 })).toThrow();
    expect(() => schema.parse({ groupId: 'g', topic: 't', limit: 1001 })).toThrow();
    expect(() => schema.parse({ groupId: 'g', topic: 't', since: 'yesterday' })).toThrow();
  });

  it('rejects empty group ids and invalid topics', () => {
    expect(() => schema.parse({ groupId: '', topic: 't' })).toThrow();
    expect(() => schema.parse({ groupId: 'g', topic: 'bad topic!' })).toThrow();
  });
});
