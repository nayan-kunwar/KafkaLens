import type { Kafka } from 'kafkajs';
import kafkajs from 'kafkajs';
import { describe, expect, it } from 'vitest';

import { AppError } from '../../src/errors/app-error.js';
import { ConsumerGroupsService } from '../../src/services/consumer-groups-service.js';

const { AssignerProtocol } = kafkajs;

interface StubMember {
  memberId: string;
  clientId: string;
  clientHost: string;
  memberMetadata?: Buffer;
  memberAssignment?: Buffer | null;
}

interface StubAdmin {
  listGroups?: () => Promise<{ groups: { groupId: string; protocolType: string }[] }>;
  describeGroups?: (ids: string[]) => Promise<{
    groups: {
      groupId: string;
      state: string;
      protocolType: string;
      protocol: string;
      members: StubMember[];
    }[];
  }>;
}

function serviceWith(stub: StubAdmin): ConsumerGroupsService {
  const admin = {
    connect: () => Promise.resolve(),
    disconnect: () => Promise.resolve(),
    ...stub,
  };
  const kafka = { admin: () => admin };
  return new ConsumerGroupsService(kafka as unknown as Kafka);
}

function assignmentBuffer(assignment: Record<string, number[]>): Buffer {
  return AssignerProtocol.MemberAssignment.encode({
    version: 1,
    assignment,
    userData: Buffer.alloc(0),
  });
}

function member(memberId: string, overrides: Partial<StubMember> = {}): StubMember {
  return {
    memberId,
    clientId: `client-${memberId}`,
    clientHost: '/127.0.0.1',
    memberAssignment: assignmentBuffer({ orders: [0] }),
    ...overrides,
  };
}

function group(
  groupId: string,
  members: StubMember[] = [],
  overrides: Partial<{ state: string; protocolType: string; protocol: string }> = {},
): {
  groupId: string;
  state: string;
  protocolType: string;
  protocol: string;
  members: StubMember[];
} {
  return {
    groupId,
    state: 'Stable',
    protocolType: 'consumer',
    protocol: 'range',
    members,
    ...overrides,
  };
}

async function expectAppError(promise: Promise<unknown>, code: string): Promise<AppError> {
  let thrown: unknown;
  try {
    await promise;
  } catch (err) {
    thrown = err;
  }
  expect(thrown).toBeInstanceOf(AppError);
  const error = thrown as AppError;
  expect(error.code).toBe(code);
  return error;
}

describe('listConsumerGroups', () => {
  const stub: StubAdmin = {
    listGroups: () =>
      Promise.resolve({
        groups: [
          { groupId: 'orders-worker', protocolType: 'consumer' },
          { groupId: 'payments-worker', protocolType: 'consumer' },
          { groupId: 'other-protocol', protocolType: 'other' },
        ],
      }),
    describeGroups: (ids: string[]) =>
      Promise.resolve({
        groups: ids.map((groupId) =>
          group(groupId, groupId === 'orders-worker' ? [member('m1'), member('m2')] : []),
        ),
      }),
  };

  it('sorts results and includes state and memberCount', async () => {
    const result = await serviceWith(stub).listConsumerGroups({ limit: 100 });
    expect(result.groups.map((g) => g.groupId)).toEqual([
      'orders-worker',
      'other-protocol',
      'payments-worker',
    ]);
    expect(result.groups.find((g) => g.groupId === 'orders-worker')).toMatchObject({
      protocolType: 'consumer',
      state: 'Stable',
      memberCount: 2,
    });
    expect(result.total).toBe(3);
    expect(result.truncated).toBe(false);
    expect(() => new Date(result.observedAt).toISOString()).not.toThrow();
  });

  it('filters by literal substring and protocol type', async () => {
    const result = await serviceWith(stub).listConsumerGroups({
      groupIdContains: 'worker',
      protocolType: 'consumer',
      limit: 100,
    });
    expect(result.groups.map((g) => g.groupId)).toEqual(['orders-worker', 'payments-worker']);
    expect(result.total).toBe(2);
  });

  it('caps at limit and reports truncation', async () => {
    const result = await serviceWith(stub).listConsumerGroups({ limit: 1 });
    expect(result.groups).toHaveLength(1);
    expect(result.total).toBe(3);
    expect(result.truncated).toBe(true);
  });

  it('returns empty without describing when nothing matches', async () => {
    let described = 0;
    const service = serviceWith({
      listGroups: () => Promise.resolve({ groups: [] }),
      describeGroups: () => {
        described += 1;
        return Promise.resolve({ groups: [] });
      },
    });
    const result = await service.listConsumerGroups({ groupIdContains: 'nope', limit: 100 });
    expect(result).toMatchObject({ groups: [], total: 0, truncated: false });
    expect(described).toBe(0);
  });

  it('propagates a batch describe failure instead of zeroing memberCount', async () => {
    const service = serviceWith({
      listGroups: () =>
        Promise.resolve({ groups: [{ groupId: 'gone', protocolType: 'consumer' }] }),
      describeGroups: () => Promise.reject(new Error('GROUP_ID_NOT_FOUND')),
    });
    await expectAppError(service.listConsumerGroups({ limit: 100 }), 'KAFKA_ERROR');
  });
});

describe('getConsumerGroup', () => {
  it('maps members sorted by memberId and caps at 500', async () => {
    const members = Array.from({ length: 501 }, (_, i) =>
      member(`m-${String(i).padStart(3, '0')}`),
    );
    const service = serviceWith({
      describeGroups: () => Promise.resolve({ groups: [group('big', members)] }),
    });
    const result = await service.getConsumerGroup('big');
    expect(result.groupId).toBe('big');
    expect(result.state).toBe('Stable');
    expect(result.memberCount).toBe(501);
    expect(result.members).toHaveLength(500);
    expect(result.truncated).toBe(true);
    expect(result.members[0]?.memberId).toBe('m-000');
  });

  it('returns observed rebalance state without interpretation', async () => {
    const service = serviceWith({
      describeGroups: () =>
        Promise.resolve({ groups: [group('rebalancing', [], { state: 'PreparingRebalance' })] }),
    });
    const result = await service.getConsumerGroup('rebalancing');
    expect(result.state).toBe('PreparingRebalance');
    expect(result.memberCount).toBe(0);
    expect(result.members).toEqual([]);
  });

  it('throws NOT_FOUND when the group is absent from the response', async () => {
    const service = serviceWith({ describeGroups: () => Promise.resolve({ groups: [] }) });
    const error = await expectAppError(service.getConsumerGroup('missing'), 'NOT_FOUND');
    expect(error.message).toContain('missing');
  });

  it('throws NOT_FOUND for a Dead group, as returned for unknown ids', async () => {
    const service = serviceWith({
      describeGroups: () => Promise.resolve({ groups: [group('missing', [], { state: 'Dead' })] }),
    });
    const error = await expectAppError(service.getConsumerGroup('missing'), 'NOT_FOUND');
    expect(error.context).toMatchObject({ groupId: 'missing', state: 'Dead' });
    await expectAppError(service.getConsumerAssignments('missing'), 'NOT_FOUND');
  });
});

describe('getConsumerAssignments', () => {
  it('decodes assignments sorted by topic and partition', async () => {
    const service = serviceWith({
      describeGroups: () =>
        Promise.resolve({
          groups: [
            group('g', [
              member('m1', { memberAssignment: assignmentBuffer({ b: [2, 0], a: [1] }) }),
            ]),
          ],
        }),
    });
    const result = await service.getConsumerAssignments('g');
    expect(result.memberCount).toBe(1);
    expect(result.assignments).toEqual([
      {
        memberId: 'm1',
        assignment: [
          { topic: 'a', partitions: [1] },
          { topic: 'b', partitions: [0, 2] },
        ],
      },
    ]);
    expect(result.truncated).toBe(false);
  });

  it('returns null for missing or undecodable buffers, empty array for empty decode', async () => {
    const service = serviceWith({
      describeGroups: () =>
        Promise.resolve({
          groups: [
            group('g', [
              member('m-missing', { memberAssignment: null }),
              member('m-empty', { memberAssignment: Buffer.alloc(0) }),
              member('m-garbage', { memberAssignment: Buffer.from([0xde, 0xad, 0xbe, 0xef]) }),
              member('m-none', { memberAssignment: assignmentBuffer({}) }),
            ]),
          ],
        }),
    });
    const result = await service.getConsumerAssignments('g');
    const byId = new Map(result.assignments.map((row) => [row.memberId, row.assignment]));
    expect(byId.get('m-missing')).toBeNull();
    expect(byId.get('m-empty')).toBeNull();
    expect(byId.get('m-garbage')).toBeNull();
    expect(byId.get('m-none')).toEqual([]);
  });

  it('throws NOT_FOUND for an unknown group', async () => {
    const service = serviceWith({ describeGroups: () => Promise.resolve({ groups: [] }) });
    await expectAppError(service.getConsumerAssignments('missing'), 'NOT_FOUND');
  });
});
