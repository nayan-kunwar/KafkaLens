import { describe, expect, it } from 'vitest';
import { z } from 'zod';

import {
  getPartitionInfoShape,
  getTopicMetadataShape,
  listTopicsShape,
} from '../../src/schemas/topic.js';

const listTopics = z.object(listTopicsShape);
const topicMetadata = z.object(getTopicMetadataShape);
const partitionInfo = z.object(getPartitionInfoShape);

describe('listTopicsShape', () => {
  it('applies defaults', () => {
    const parsed = listTopics.parse({});
    expect(parsed).toEqual({ limit: 100 });
  });

  it('coerces limit from string', () => {
    expect(listTopics.parse({ limit: '20' })).toEqual({ limit: 20 });
  });

  it('accepts nameContains', () => {
    expect(listTopics.parse({ nameContains: 'orders' })).toEqual({
      nameContains: 'orders',
      limit: 100,
    });
  });

  it.each([0, 501, -1, 1.5])('rejects limit %s', (limit) => {
    expect(listTopics.safeParse({ limit }).success).toBe(false);
  });

  it.each(['', 'x'.repeat(101)])('rejects nameContains %s', (nameContains) => {
    expect(listTopics.safeParse({ nameContains }).success).toBe(false);
  });
});

describe('topicName schema', () => {
  it.each(['orders', 'a.b_c-1', 'x'.repeat(249)])('accepts %s', (topic) => {
    expect(topicMetadata.safeParse({ topic }).success).toBe(true);
  });

  it.each([
    ['empty', ''],
    ['dot', '.'],
    ['dotdot', '..'],
    ['too long', 'x'.repeat(250)],
    ['space', 'has space'],
    ['slash', 'a/b'],
    ['unicode', 'töpic'],
  ])('rejects %s', (_label, topic) => {
    expect(topicMetadata.safeParse({ topic }).success).toBe(false);
  });
});

describe('getPartitionInfoShape', () => {
  it('accepts topic without partition', () => {
    expect(partitionInfo.parse({ topic: 'orders' })).toEqual({ topic: 'orders' });
  });

  it('coerces partition from string', () => {
    expect(partitionInfo.parse({ topic: 'orders', partition: '3' })).toEqual({
      topic: 'orders',
      partition: 3,
    });
  });

  it.each([-1, 1.5])('rejects partition %s', (partition) => {
    expect(partitionInfo.safeParse({ topic: 'orders', partition }).success).toBe(false);
  });
});
