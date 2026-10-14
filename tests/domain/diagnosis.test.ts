import { describe, expect, it } from 'vitest';

import {
  diagnose,
  type DiagnosisPartition,
  type DiagnosisSnapshot,
} from '../../src/domain/diagnosis.js';
import { rankFindings, type Finding } from '../../src/domain/findings.js';

const now = new Date('2026-01-01T00:05:00.000Z');

function row(overrides: Partial<DiagnosisPartition> = {}): DiagnosisPartition {
  return {
    partitionId: 0,
    highWatermark: '10',
    committedOffset: '10',
    lag: '0',
    assigned: true,
    samples: [],
    ...overrides,
  };
}

function snapshot(overrides: Partial<DiagnosisSnapshot> = {}): DiagnosisSnapshot {
  return {
    groupId: 'payments',
    topic: 'payment-events',
    groupState: 'Stable',
    memberCount: 1,
    partitions: [row()],
    historyAvailable: true,
    imbalanceRatio: 5,
    stallWindowSeconds: 300,
    now,
    coverageIncomplete: false,
    ...overrides,
  };
}

describe('rankFindings', () => {
  it('orders by severity, then confidence, then type', () => {
    const findings: Finding[] = [
      { type: 'UNKNOWN', severity: 'low', confidence: 0.9, evidence: [], recommendations: [] },
      {
        type: 'CONSUMER_SLOW',
        severity: 'medium',
        confidence: 0.4,
        evidence: [],
        recommendations: [],
      },
      {
        type: 'CONSUMER_STALLED',
        severity: 'high',
        confidence: 0.5,
        evidence: [],
        recommendations: [],
      },
      {
        type: 'PARTITION_IMBALANCE',
        severity: 'high',
        confidence: 0.9,
        evidence: [],
        recommendations: [],
      },
    ];
    expect(rankFindings(findings).map((finding) => finding.type)).toEqual([
      'PARTITION_IMBALANCE',
      'CONSUMER_STALLED',
      'CONSUMER_SLOW',
      'UNKNOWN',
    ]);
  });
});

describe('diagnose', () => {
  it('reports no issue when known lag is zero on a stable group', () => {
    const result = diagnose(snapshot());
    expect(result.status).toBe('no_issue_detected');
    expect(result.findings).toEqual([]);
    expect(result.confidence).toBeNull();
    expect(result.impact.totalLag).toBe('0');
  });

  it('does not treat an empty topic with no commits as missing offsets', () => {
    const result = diagnose(
      snapshot({
        partitions: [
          row({
            highWatermark: '0',
            committedOffset: null,
            lag: null,
            unknownReason: 'NO_COMMITTED_OFFSET',
          }),
        ],
      }),
    );
    expect(result.findings.map((finding) => finding.type)).not.toContain('NO_COMMITTED_OFFSET');
    expect(result.status).toBe('no_issue_detected');
  });

  it('reports NO_COMMITTED_OFFSET when messages exist and nothing is committed', () => {
    const result = diagnose(
      snapshot({
        partitions: [
          row({
            highWatermark: '25',
            committedOffset: null,
            lag: null,
            unknownReason: 'NO_COMMITTED_OFFSET',
          }),
        ],
      }),
    );
    expect(result.findings.map((finding) => finding.type)).toContain('NO_COMMITTED_OFFSET');
    expect(result.evidence.some((statement) => statement.kind === 'observed_fact')).toBe(true);
    expect(result.evidence.some((statement) => statement.kind === 'recommendation')).toBe(true);
    expect(result.impact.totalLag).toBeNull();
  });

  it('reports insufficient capacity when nobody is joined and lag is known', () => {
    const result = diagnose(
      snapshot({
        memberCount: 0,
        partitions: [
          row({ lag: '40', committedOffset: '10', highWatermark: '50', assigned: false }),
        ],
      }),
    );
    const finding = result.findings.find(
      (entry) => entry.type === 'INSUFFICIENT_CONSUMER_CAPACITY',
    );
    expect(finding?.severity).toBe('high');
    expect(finding?.evidence.join(' ')).toMatch(/does not by itself show that a process crashed/);
    expect(result.findings.map((entry) => entry.type)).not.toContain('CONSUMER_STALLED');
  });

  it('withholds stall findings while the group is rebalancing', () => {
    const result = diagnose(
      snapshot({
        groupState: 'PreparingRebalance',
        partitions: [
          row({
            lag: '30',
            committedOffset: '10',
            highWatermark: '40',
            samples: [
              {
                observedAt: '2026-01-01T00:00:00.000Z',
                highWatermark: '20',
                committedOffset: '10',
                lag: '10',
              },
              {
                observedAt: '2026-01-01T00:05:00.000Z',
                highWatermark: '40',
                committedOffset: '10',
                lag: '30',
              },
            ],
          }),
        ],
      }),
    );
    expect(result.findings.map((finding) => finding.type)).toContain('REBALANCING');
    expect(result.findings.map((finding) => finding.type)).not.toContain('CONSUMER_STALLED');
    expect(result.stall.finding?.type).toBe('CONSUMER_STALLED');
    expect(result.evidence.some((statement) => statement.text.includes('withheld'))).toBe(true);
  });

  it('returns UNKNOWN when lag is positive and history cannot classify it', () => {
    const result = diagnose(
      snapshot({
        historyAvailable: false,
        partitions: [row({ lag: '15', committedOffset: '5', highWatermark: '20', samples: [] })],
      }),
    );
    expect(result.status).toBe('insufficient_evidence');
    expect(result.findings.map((finding) => finding.type)).toEqual(['UNKNOWN']);
    expect(result.likelyCauses[0]).toMatch(/UNKNOWN/);
  });

  it('ranks a stall above a slower secondary finding', () => {
    const result = diagnose(
      snapshot({
        partitions: [
          row({
            partitionId: 0,
            lag: '10',
            committedOffset: '10',
            highWatermark: '20',
            samples: [
              {
                observedAt: '2026-01-01T00:00:00.000Z',
                highWatermark: '15',
                committedOffset: '10',
                lag: '5',
              },
              {
                observedAt: '2026-01-01T00:05:00.000Z',
                highWatermark: '20',
                committedOffset: '10',
                lag: '10',
              },
            ],
          }),
          row({
            partitionId: 1,
            lag: '1000',
            committedOffset: '0',
            highWatermark: '1000',
            samples: [
              {
                observedAt: '2026-01-01T00:00:00.000Z',
                highWatermark: '100',
                committedOffset: '0',
                lag: '100',
              },
              {
                observedAt: '2026-01-01T00:05:00.000Z',
                highWatermark: '1000',
                committedOffset: '0',
                lag: '1000',
              },
            ],
          }),
        ],
      }),
    );
    expect(result.findings[0]?.type).toBe('PARTITION_IMBALANCE');
    expect(result.findings.map((finding) => finding.type)).toContain('CONSUMER_STALLED');
    expect(result.status).toBe('attention');
    expect(result.confidence).toBe(result.findings[0]?.confidence);
  });

  it('lowers confidence when coverage is incomplete', () => {
    const complete = diagnose(
      snapshot({
        partitions: [
          row({
            highWatermark: '25',
            committedOffset: null,
            lag: null,
            unknownReason: 'NO_COMMITTED_OFFSET',
          }),
        ],
      }),
    );
    const incomplete = diagnose(
      snapshot({
        coverageIncomplete: true,
        partitions: [
          row({
            highWatermark: '25',
            committedOffset: null,
            lag: null,
            unknownReason: 'NO_COMMITTED_OFFSET',
          }),
        ],
      }),
    );
    const full = complete.findings[0]?.confidence ?? 0;
    const reduced = incomplete.findings[0]?.confidence ?? 0;
    expect(reduced).toBeLessThan(full);
  });
});
