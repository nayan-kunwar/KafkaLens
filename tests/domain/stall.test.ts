import { describe, expect, it } from 'vitest';

import { analyzeConsumerProgress, type StallPartitionInput } from '../../src/domain/stall.js';

const now = new Date('2026-01-01T00:05:00.000Z');

function partition(overrides: Partial<StallPartitionInput> = {}): StallPartitionInput {
  return {
    partitionId: 0,
    assigned: true,
    lag: '20',
    samples: [
      {
        observedAt: '2026-01-01T00:00:00.000Z',
        highWatermark: '40',
        committedOffset: '20',
        lag: '20',
      },
      {
        observedAt: '2026-01-01T00:05:00.000Z',
        highWatermark: '50',
        committedOffset: '20',
        lag: '30',
      },
    ],
    ...overrides,
  };
}

describe('analyzeConsumerProgress', () => {
  it('classifies an assigned consumer with unchanged offsets and lag as not progressing', () => {
    const analysis = analyzeConsumerProgress([partition()], 300, now);
    expect(analysis.partitions[0]?.progress).toBe('ASSIGNED_NOT_PROGRESSING');
    expect(analysis.groupProgress).toBe('ASSIGNED_NOT_PROGRESSING');
    expect(analysis.finding?.type).toBe('CONSUMER_STALLED');
    expect(analysis.finding?.evidence.join(' ')).toMatch(/does not show that the process crashed/);
    expect(analysis.finding?.recommendations.join(' ')).toMatch(
      /not treat a lack of offset progress as proof that the process has crashed/,
    );
  });

  it('does not call a caught-up consumer stalled when the offset is unchanged', () => {
    const analysis = analyzeConsumerProgress([partition({ lag: '0' })], 300, now);
    expect(analysis.partitions[0]?.progress).toBe('ACTIVELY_PROCESSING');
    expect(analysis.finding).toBeNull();
  });

  it('classifies an unassigned partition separately from a stall', () => {
    const analysis = analyzeConsumerProgress([partition({ assigned: false })], 300, now);
    expect(analysis.partitions[0]?.progress).toBe('NOT_ASSIGNED');
    expect(analysis.groupProgress).toBe('NOT_ASSIGNED');
    expect(analysis.finding).toBeNull();
  });

  it('does not classify progress when the assignment is unknown', () => {
    const analysis = analyzeConsumerProgress([partition({ assigned: null })], 300, now);
    expect(analysis.partitions[0]?.progress).toBe('INSUFFICIENT_EVIDENCE');
    expect(analysis.finding).toBeNull();
  });

  it('classifies advancing offsets with growing lag as slow', () => {
    const analysis = analyzeConsumerProgress(
      [
        partition({
          samples: [
            {
              observedAt: '2026-01-01T00:00:00.000Z',
              highWatermark: '100',
              committedOffset: '10',
              lag: '20',
            },
            {
              observedAt: '2026-01-01T00:05:00.000Z',
              highWatermark: '200',
              committedOffset: '40',
              lag: '80',
            },
          ],
          lag: '80',
        }),
      ],
      300,
      now,
    );
    expect(analysis.partitions[0]?.progress).toBe('PROGRESSING_SLOWLY');
    expect(analysis.slowFinding?.type).toBe('CONSUMER_SLOW');
    expect(analysis.finding).toBeNull();
  });

  it('classifies advancing offsets without lag growth as active', () => {
    const analysis = analyzeConsumerProgress(
      [
        partition({
          lag: '5',
          samples: [
            {
              observedAt: '2026-01-01T00:00:00.000Z',
              highWatermark: '20',
              committedOffset: '10',
              lag: '10',
            },
            {
              observedAt: '2026-01-01T00:05:00.000Z',
              highWatermark: '30',
              committedOffset: '25',
              lag: '5',
            },
          ],
        }),
      ],
      300,
      now,
    );
    expect(analysis.partitions[0]?.progress).toBe('ACTIVELY_PROCESSING');
    expect(analysis.slowFinding).toBeNull();
  });

  it('returns insufficient evidence when samples do not cover the stall window', () => {
    const analysis = analyzeConsumerProgress(
      [
        partition({
          samples: [
            {
              observedAt: '2026-01-01T00:04:50.000Z',
              highWatermark: '20',
              committedOffset: '10',
              lag: '10',
            },
            {
              observedAt: '2026-01-01T00:05:00.000Z',
              highWatermark: '21',
              committedOffset: '10',
              lag: '11',
            },
          ],
        }),
      ],
      300,
      now,
    );
    expect(analysis.partitions[0]?.progress).toBe('INSUFFICIENT_EVIDENCE');
    expect(analysis.finding).toBeNull();
  });

  it('does not treat a decreased committed offset as a stall', () => {
    const analysis = analyzeConsumerProgress(
      [
        partition({
          samples: [
            {
              observedAt: '2026-01-01T00:00:00.000Z',
              highWatermark: '100',
              committedOffset: '80',
              lag: '20',
            },
            {
              observedAt: '2026-01-01T00:05:00.000Z',
              highWatermark: '100',
              committedOffset: '5',
              lag: '95',
            },
          ],
        }),
      ],
      300,
      now,
    );
    expect(analysis.partitions[0]?.progress).toBe('INSUFFICIENT_EVIDENCE');
    expect(analysis.finding).toBeNull();
  });

  it('does not invent a stall when lag is unknown', () => {
    const analysis = analyzeConsumerProgress([partition({ lag: null })], 300, now);
    expect(analysis.partitions[0]?.progress).toBe('INSUFFICIENT_EVIDENCE');
    expect(analysis.finding).toBeNull();
  });
});
