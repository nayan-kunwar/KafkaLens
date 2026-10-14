import { describe, expect, it } from 'vitest';

import { AppError } from '../../src/errors/app-error.js';
import type { IncidentStore } from '../../src/persistence/incident-store.js';
import type { LagStore } from '../../src/persistence/lag-store.js';
import type { AdminService } from '../../src/services/admin-service.js';
import type { ConsumerGroupsService } from '../../src/services/consumer-groups-service.js';
import type { CorrelationService } from '../../src/services/correlation-service.js';
import { DiagnosisService } from '../../src/services/diagnosis-service.js';
import type { OffsetsService, TopicLagResult } from '../../src/services/offsets-service.js';

const observedAt = '2026-01-01T00:05:00.000Z';
const now = () => new Date(observedAt);

function lag(overrides: Partial<TopicLagResult> = {}): TopicLagResult {
  return {
    groupId: 'payments',
    topic: 'payment-events',
    groupState: 'Stable',
    partitionCount: 1,
    partitions: [
      {
        partitionId: 0,
        highWatermark: '40',
        committedOffset: '10',
        lag: '30',
      },
    ],
    totalLag: '30',
    unknownPartitionCount: 0,
    orphanedCommits: [],
    truncated: false,
    observedAt,
    ...overrides,
  };
}

function service(options: {
  historyError?: boolean;
  groupError?: AppError;
  record?: (incident: unknown) => Promise<'stored' | 'duplicate'>;
}): DiagnosisService {
  const admin = {
    getTopicMetadata: () =>
      Promise.resolve({
        name: 'payment-events',
        partitionCount: 1,
        partitions: [],
        observedAt,
      }),
    getClusterInfo: () =>
      Promise.resolve({
        clusterId: 'cluster-1',
        controllerId: 1,
        brokerCount: 1,
        brokers: [],
        observedAt,
      }),
  };
  const groups = {
    getConsumerGroup: () => {
      if (options.groupError) return Promise.reject(options.groupError);
      return Promise.resolve({
        groupId: 'payments',
        state: 'Stable',
        protocolType: 'consumer',
        protocol: 'range',
        memberCount: 1,
        members: [],
        truncated: false,
        observedAt,
      });
    },
    getConsumerAssignments: () =>
      Promise.resolve({
        groupId: 'payments',
        memberCount: 1,
        assignments: [
          { memberId: 'member-1', assignment: [{ topic: 'payment-events', partitions: [0] }] },
        ],
        truncated: false,
        observedAt,
      }),
  };
  const offsets = {
    getConsumerLag: () => Promise.resolve(lag()),
  };
  const history = {
    queryHistory: () => {
      if (options.historyError) {
        return Promise.reject(new AppError('STORE_UNAVAILABLE', 'PostgreSQL read failed'));
      }
      return Promise.resolve({
        observations: [
          {
            partition: 0,
            observedAt: '2026-01-01T00:00:00.000Z',
            highWatermark: '20',
            committedOffset: '10',
            lag: '10',
          },
        ],
        truncated: false,
      });
    },
  };
  const incidents = {
    findSimilar: () => Promise.resolve([]),
    record: (incident: unknown) => options.record?.(incident) ?? Promise.resolve('stored' as const),
    getById: () => Promise.reject(new AppError('NOT_FOUND', 'missing')),
  };
  const correlation = {
    correlate: () =>
      Promise.resolve({
        status: 'not_configured' as const,
        events: [],
        message: 'GitHub repository and deployment source are not configured.',
      }),
  };
  return new DiagnosisService(
    admin as unknown as AdminService,
    groups as unknown as ConsumerGroupsService,
    offsets as unknown as OffsetsService,
    history as unknown as LagStore,
    incidents as unknown as IncidentStore,
    correlation as unknown as CorrelationService,
    { imbalanceRatio: 5, stallWindowSeconds: 300, now },
  );
}

describe('DiagnosisService', () => {
  it('returns a stall finding and stores an incident when history shows no progress', async () => {
    const stored: unknown[] = [];
    const report = await service({
      record: (incident) => {
        stored.push(incident);
        return Promise.resolve('stored');
      },
    }).diagnoseConsumerGroup('payments', 'payment-events');
    expect(report.status).toBe('attention');
    expect(report.findings.map((finding) => finding.type)).toContain('CONSUMER_STALLED');
    expect(report.assignmentsKnown).toBe(true);
    expect(report.partialFailures).toEqual([]);
    expect(report.relatedIncidents.status).toBe('ok');
    expect(stored).toHaveLength(1);
    expect(report.evidence.some((statement) => statement.kind === 'observed_fact')).toBe(true);
    expect(report.evidence.some((statement) => statement.kind === 'inference')).toBe(true);
  });

  it('keeps the diagnosis when lag history is unavailable', async () => {
    const report = await service({ historyError: true }).diagnoseConsumerGroup(
      'payments',
      'payment-events',
    );
    expect(report.partialFailures.map((failure) => failure.source)).toContain('lag_history');
    expect(report.timeline.historyAvailable).toBe(false);
    expect(report.groupId).toBe('payments');
  });

  it('propagates a missing consumer group', async () => {
    await expect(
      service({
        groupError: new AppError('NOT_FOUND', 'Consumer group "payments" not found'),
      }).diagnoseConsumerGroup('payments', 'payment-events'),
    ).rejects.toMatchObject({ code: 'NOT_FOUND' });
  });

  it('does not fail the report when incident storage fails', async () => {
    const report = await service({
      record: () => Promise.reject(new AppError('STORE_UNAVAILABLE', 'PostgreSQL write failed')),
    }).diagnoseConsumerGroup('payments', 'payment-events');
    expect(report.findings.length).toBeGreaterThan(0);
    expect(report.partialFailures.map((failure) => failure.source)).toContain('incidents');
  });
});
