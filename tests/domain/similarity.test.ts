import { describe, expect, it } from 'vitest';

import { jaccard, scoreIncident } from '../../src/domain/similarity.js';

describe('scoreIncident', () => {
  it('scores group, topic, and finding overlap without a model', () => {
    expect(
      scoreIncident(
        { groupId: 'g', topic: 't', findingTypes: ['CONSUMER_STALLED', 'PARTITION_IMBALANCE'] },
        { consumerGroup: 'g', topic: 't', findingTypes: ['CONSUMER_STALLED'] },
      ),
    ).toBe(0.9);

    expect(
      scoreIncident(
        { groupId: 'g', topic: 't', findingTypes: ['CONSUMER_STALLED'] },
        { consumerGroup: 'other', topic: 'other', findingTypes: ['REBALANCING'] },
      ),
    ).toBe(0);
  });

  it('computes Jaccard on the finding sets', () => {
    expect(jaccard(['A', 'B'], ['B', 'C'])).toBeCloseTo(1 / 3);
    expect(jaccard([], [])).toBe(0);
  });
});
