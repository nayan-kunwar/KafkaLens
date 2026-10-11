import { describe, expect, it } from 'vitest';
import { z } from 'zod';

import { getConsumerLagShape } from '../../src/schemas/lag.js';

describe('getConsumerLagShape', () => {
  const schema = z.object(getConsumerLagShape);

  it('accepts a group id alone for the group-wide summary', () => {
    expect(schema.parse({ groupId: 'orders-worker' })).toEqual({ groupId: 'orders-worker' });
  });

  it('accepts a group id with a topic for per-partition lag', () => {
    expect(schema.parse({ groupId: 'orders-worker', topic: 'orders' })).toEqual({
      groupId: 'orders-worker',
      topic: 'orders',
    });
  });

  it('rejects empty or over-long group ids', () => {
    expect(() => schema.parse({ groupId: '' })).toThrow();
    expect(() => schema.parse({ groupId: 'g'.repeat(256) })).toThrow();
  });

  it('rejects invalid topic names', () => {
    expect(() => schema.parse({ groupId: 'g', topic: 'bad topic!' })).toThrow();
    expect(() => schema.parse({ groupId: 'g', topic: '' })).toThrow();
  });
});
