import { describe, expect, it } from 'vitest';

import { calculatePartitionLag, sumLags } from '../../src/domain/lag.js';

describe('calculatePartitionLag', () => {
  it('computes high minus committed as a decimal string', () => {
    expect(calculatePartitionLag('100', '40')).toEqual({ lag: '60' });
    expect(calculatePartitionLag('0', '0')).toEqual({ lag: '0' });
  });

  it('handles values beyond Number.MAX_SAFE_INTEGER with BigInt', () => {
    expect(calculatePartitionLag('9007199254740993', '1')).toEqual({ lag: '9007199254740992' });
  });

  it('reports unknown for a missing high watermark', () => {
    expect(calculatePartitionLag(null, '10')).toEqual({
      lag: null,
      unknownReason: 'UNKNOWN_WATERMARK',
    });
    expect(calculatePartitionLag(undefined, '10')).toEqual({
      lag: null,
      unknownReason: 'UNKNOWN_WATERMARK',
    });
  });

  it('reports unknown for a missing committed offset or the -1 sentinel', () => {
    expect(calculatePartitionLag('100', null)).toEqual({
      lag: null,
      unknownReason: 'NO_COMMITTED_OFFSET',
    });
    expect(calculatePartitionLag('100', undefined)).toEqual({
      lag: null,
      unknownReason: 'NO_COMMITTED_OFFSET',
    });
    expect(calculatePartitionLag('100', '-1')).toEqual({
      lag: null,
      unknownReason: 'NO_COMMITTED_OFFSET',
    });
    // A zero high watermark with no commit is still unknown, not zero lag.
    expect(calculatePartitionLag('0', '-1')).toEqual({
      lag: null,
      unknownReason: 'NO_COMMITTED_OFFSET',
    });
  });

  it('clamps a negative difference to zero and flags the adjustment', () => {
    expect(calculatePartitionLag('50', '80')).toEqual({ lag: '0', adjusted: true });
  });

  it('reports invalid non-decimal offset strings as unknown', () => {
    expect(calculatePartitionLag('abc', '10')).toEqual({
      lag: null,
      unknownReason: 'INVALID_OFFSET',
    });
    expect(calculatePartitionLag('100', '4.5')).toEqual({
      lag: null,
      unknownReason: 'INVALID_OFFSET',
    });
    expect(calculatePartitionLag('100', '-5')).toEqual({
      lag: null,
      unknownReason: 'INVALID_OFFSET',
    });
    expect(calculatePartitionLag('-3', '10')).toEqual({
      lag: null,
      unknownReason: 'INVALID_OFFSET',
    });
  });
});

describe('sumLags', () => {
  it('sums known lags and counts unknowns', () => {
    expect(sumLags(['10', null, '5', null])).toEqual({ total: '15', unknownCount: 2 });
  });

  it('returns null total when every partition is unknown', () => {
    expect(sumLags([null, null])).toEqual({ total: null, unknownCount: 2 });
  });

  it('returns null total for an empty input', () => {
    expect(sumLags([])).toEqual({ total: null, unknownCount: 0 });
  });

  it('sums values beyond Number.MAX_SAFE_INTEGER', () => {
    expect(sumLags(['9007199254740993', '2'])).toEqual({
      total: '9007199254740995',
      unknownCount: 0,
    });
  });
});
