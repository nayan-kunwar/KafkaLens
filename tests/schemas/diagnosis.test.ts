import { describe, expect, it } from 'vitest';
import { z } from 'zod';

import {
  diagnoseConsumerGroupShape,
  findSimilarIncidentsShape,
} from '../../src/schemas/diagnosis.js';

describe('diagnosis tool schemas', () => {
  const diagnoseSchema = z.object(diagnoseConsumerGroupShape);
  const similarSchema = z.object(findSimilarIncidentsShape);

  it('requires a group and a topic', () => {
    expect(diagnoseSchema.parse({ groupId: 'g', topic: 'orders' })).toEqual({
      groupId: 'g',
      topic: 'orders',
    });
    expect(() => diagnoseSchema.parse({ groupId: '', topic: 'orders' })).toThrow();
    expect(() => diagnoseSchema.parse({ groupId: 'g', topic: 'bad topic' })).toThrow();
  });

  it('bounds similar-incident queries', () => {
    expect(similarSchema.parse({ groupId: 'g', topic: 'orders' })).toMatchObject({ limit: 10 });
    expect(
      similarSchema.parse({
        groupId: 'g',
        topic: 'orders',
        findingTypes: ['CONSUMER_STALLED'],
        limit: 5,
      }).findingTypes,
    ).toEqual(['CONSUMER_STALLED']);
    expect(() =>
      similarSchema.parse({ groupId: 'g', topic: 'orders', findingTypes: ['NOT_A_FINDING'] }),
    ).toThrow();
    expect(() => similarSchema.parse({ groupId: 'g', topic: 'orders', limit: 51 })).toThrow();
  });
});
