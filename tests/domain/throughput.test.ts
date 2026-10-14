import { describe, expect, it } from 'vitest';

import { analyzeThroughput } from '../../src/domain/throughput.js';

const now = new Date('2026-01-01T00:01:00.000Z');

describe('analyzeThroughput', () => {
  it('estimates incoming, processing, and lag growth from offset deltas', () => {
    const analysis = analyzeThroughput(
      [
        {
          partitionId: 0,
          samples: [
            {
              observedAt: '2026-01-01T00:00:00.000Z',
              highWatermark: '100',
              committedOffset: '40',
              lag: '60',
            },
            {
              observedAt: '2026-01-01T00:01:00.000Z',
              highWatermark: '220',
              committedOffset: '100',
              lag: '120',
            },
          ],
        },
      ],
      now,
      300,
    );
    expect(analysis.incoming).toMatchObject({
      messagesPerSecond: 2,
      label: 'estimate',
      insufficientEvidence: false,
    });
    expect(analysis.processing.messagesPerSecond).toBe(1);
    expect(analysis.lagGrowth.messagesPerSecond).toBe(1);
    expect(analysis.producerOutpacing).toBe(true);
    expect(analysis.finding?.type).toBe('PRODUCER_OUTPACING_CONSUMER');
    expect(analysis.evidence.join(' ')).toMatch(/estimate/i);
    expect(analysis.evidence.join(' ')).toMatch(/not broker producer metrics/i);
  });

  it('returns insufficient evidence for a single sample', () => {
    const analysis = analyzeThroughput(
      [
        {
          partitionId: 0,
          samples: [
            {
              observedAt: '2026-01-01T00:01:00.000Z',
              highWatermark: '10',
              committedOffset: '4',
              lag: '6',
            },
          ],
        },
      ],
      now,
      300,
    );
    expect(analysis.incoming.insufficientEvidence).toBe(true);
    expect(analysis.processing.messagesPerSecond).toBeNull();
    expect(analysis.producerOutpacing).toBeNull();
    expect(analysis.finding).toBeNull();
  });

  it('excludes a backwards watermark instead of treating it as throughput', () => {
    const analysis = analyzeThroughput(
      [
        {
          partitionId: 0,
          samples: [
            {
              observedAt: '2026-01-01T00:00:00.000Z',
              highWatermark: '50',
              committedOffset: '10',
              lag: '40',
            },
            {
              observedAt: '2026-01-01T00:01:00.000Z',
              highWatermark: '20',
              committedOffset: '5',
              lag: '15',
            },
          ],
        },
      ],
      now,
      300,
    );
    expect(analysis.incoming.messagesPerSecond).toBeNull();
    expect(analysis.finding).toBeNull();
  });

  it('does not flag producer outpacing when lag is not growing', () => {
    const analysis = analyzeThroughput(
      [
        {
          partitionId: 0,
          samples: [
            {
              observedAt: '2026-01-01T00:00:00.000Z',
              highWatermark: '100',
              committedOffset: '80',
              lag: '20',
            },
            {
              observedAt: '2026-01-01T00:01:00.000Z',
              highWatermark: '160',
              committedOffset: '150',
              lag: '10',
            },
          ],
        },
      ],
      now,
      300,
    );
    expect(analysis.producerOutpacing).toBe(false);
    expect(analysis.finding).toBeNull();
  });
});
