import { describe, expect, it } from 'vitest';

import { analyzePartitionImbalance } from '../../src/domain/imbalance.js';

describe('analyzePartitionImbalance', () => {
  it('does not flag a uniform lag distribution', () => {
    const analysis = analyzePartitionImbalance(
      [
        { partitionId: 0, lag: '10' },
        { partitionId: 1, lag: '10' },
        { partitionId: 2, lag: '10' },
      ],
      5,
    );
    expect(analysis.insufficientEvidence).toBe(false);
    expect(analysis.outlierPartitionIds).toEqual([]);
    expect(analysis.finding).toBeNull();
    expect(analysis.maxMinRatio).toBe('1');
    expect(analysis.mean).toBe('10');
    expect(analysis.median).toBe('10');
    expect(analysis.totalKnownLag).toBe('30');
  });

  it('flags a partition that exceeds the configured ratio', () => {
    const analysis = analyzePartitionImbalance(
      [
        { partitionId: 0, lag: '10' },
        { partitionId: 7, lag: '1000' },
        { partitionId: 2, lag: '10' },
      ],
      5,
    );
    expect(analysis.outlierPartitionIds).toEqual([7]);
    expect(analysis.finding?.type).toBe('PARTITION_IMBALANCE');
    expect(analysis.finding?.severity).toBe('high');
    expect(analysis.shares[0]).toMatchObject({ partitionId: 7 });
    expect(analysis.evidence.some((line) => line.includes('Partition 7'))).toBe(true);
    expect(analysis.stdDev).not.toBeNull();
  });

  it('uses the mean when the minimum lag is zero', () => {
    const hot = analyzePartitionImbalance(
      [
        { partitionId: 0, lag: '0' },
        { partitionId: 1, lag: '0' },
        { partitionId: 2, lag: '0' },
        { partitionId: 3, lag: '0' },
        { partitionId: 4, lag: '100' },
      ],
      5,
    );
    expect(hot.maxMinRatio).toBeNull();
    expect(hot.outlierPartitionIds).toEqual([4]);

    const mild = analyzePartitionImbalance(
      [
        { partitionId: 0, lag: '0' },
        { partitionId: 1, lag: '0' },
        { partitionId: 2, lag: '1' },
      ],
      5,
    );
    expect(mild.outlierPartitionIds).toEqual([]);
    expect(mild.finding).toBeNull();
  });

  it('reports insufficient evidence for a single known lag and does not treat null as zero', () => {
    const analysis = analyzePartitionImbalance(
      [
        { partitionId: 0, lag: '40' },
        { partitionId: 1, lag: null },
      ],
      5,
    );
    expect(analysis.insufficientEvidence).toBe(true);
    expect(analysis.unknownPartitionCount).toBe(1);
    expect(analysis.totalKnownLag).toBeNull();
    expect(analysis.finding).toBeNull();
  });

  it('reports an even-count median', () => {
    const analysis = analyzePartitionImbalance(
      [
        { partitionId: 0, lag: '10' },
        { partitionId: 1, lag: '20' },
      ],
      100,
    );
    expect(analysis.median).toBe('15');
    expect(analysis.finding).toBeNull();
  });
});
