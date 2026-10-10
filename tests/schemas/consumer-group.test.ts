import { describe, expect, it } from 'vitest';
import { z } from 'zod';

import {
  getConsumerAssignmentsShape,
  getConsumerGroupShape,
  listConsumerGroupsShape,
} from '../../src/schemas/consumer-group.js';

describe('listConsumerGroupsShape', () => {
  const schema = z.object(listConsumerGroupsShape);

  it('defaults limit to 100', () => {
    expect(schema.parse({})).toMatchObject({ limit: 100 });
  });

  it('accepts filters and a bounded limit', () => {
    expect(
      schema.parse({ groupIdContains: 'orders', protocolType: 'consumer', limit: 10 }),
    ).toMatchObject({ groupIdContains: 'orders', protocolType: 'consumer', limit: 10 });
  });

  it('rejects out-of-range limits', () => {
    expect(() => schema.parse({ limit: 0 })).toThrow();
    expect(() => schema.parse({ limit: 501 })).toThrow();
  });
});

describe('getConsumerGroupShape', () => {
  const schema = z.object(getConsumerGroupShape);

  it('accepts group ids outside the topic-name charset', () => {
    expect(schema.parse({ groupId: 'orders:worker/v2' })).toMatchObject({
      groupId: 'orders:worker/v2',
    });
  });

  it('rejects empty and over-long group ids', () => {
    expect(() => schema.parse({ groupId: '' })).toThrow();
    expect(() => schema.parse({ groupId: 'g'.repeat(256) })).toThrow();
  });
});

describe('getConsumerAssignmentsShape', () => {
  const schema = z.object(getConsumerAssignmentsShape);

  it('accepts a group id', () => {
    expect(schema.parse({ groupId: 'orders-worker' })).toMatchObject({
      groupId: 'orders-worker',
    });
  });

  it('rejects a missing group id', () => {
    expect(() => schema.parse({})).toThrow();
  });
});
