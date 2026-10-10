import type { Admin, GroupDescription, Kafka } from 'kafkajs';
import kafkajs from 'kafkajs';

import { AppError } from '../errors/app-error.js';

import { mapKafkaError } from './kafka-error.js';

const { AssignerProtocol } = kafkajs;

const MAX_MEMBER_ROWS = 500;

export interface ConsumerGroupSummary {
  groupId: string;
  protocolType: string;
  state: string;
  memberCount: number;
}

export interface ConsumerGroupListResult {
  groups: ConsumerGroupSummary[];
  total: number;
  truncated: boolean;
  observedAt: string;
}

export interface ConsumerGroupMember {
  memberId: string;
  clientId: string;
  clientHost: string;
}

export interface ConsumerGroupResult {
  groupId: string;
  state: string;
  protocolType: string;
  protocol: string;
  memberCount: number;
  members: ConsumerGroupMember[];
  truncated: boolean;
  observedAt: string;
}

export interface TopicAssignment {
  topic: string;
  partitions: number[];
}

export interface MemberAssignmentRow {
  memberId: string;
  assignment: TopicAssignment[] | null;
}

export interface ConsumerAssignmentsResult {
  groupId: string;
  memberCount: number;
  assignments: MemberAssignmentRow[];
  truncated: boolean;
  observedAt: string;
}

export class ConsumerGroupsService {
  private readonly kafka: Kafka;

  constructor(kafka: Kafka) {
    this.kafka = kafka;
  }

  async listConsumerGroups(options: {
    groupIdContains?: string | undefined;
    protocolType?: string | undefined;
    limit: number;
  }): Promise<ConsumerGroupListResult> {
    const { groupIdContains, protocolType, limit } = options;
    return this.withAdmin(async (admin) => {
      const { groups } = await admin.listGroups();
      const filtered = groups.filter(
        (group) =>
          (groupIdContains === undefined || group.groupId.includes(groupIdContains)) &&
          (protocolType === undefined || group.protocolType === protocolType),
      );
      filtered.sort((a, b) => a.groupId.localeCompare(b.groupId));
      const total = filtered.length;
      const page = filtered.slice(0, limit);
      if (page.length === 0) {
        return { groups: [], total, truncated: false, observedAt: new Date().toISOString() };
      }
      // Option A: single batched describe for the page. A throw (e.g. a group
      // deleted mid-list) fails the whole call via mapKafkaError — no per-row nulls.
      const described = await admin.describeGroups(page.map((group) => group.groupId));
      const byId = new Map(described.groups.map((group) => [group.groupId, group]));
      return {
        groups: page.map((overview) => {
          const detail = byId.get(overview.groupId);
          return {
            groupId: overview.groupId,
            protocolType: overview.protocolType,
            state: detail?.state ?? 'Unknown',
            memberCount: detail?.members.length ?? 0,
          };
        }),
        total,
        truncated: total > page.length,
        observedAt: new Date().toISOString(),
      };
    });
  }

  async getConsumerGroup(groupId: string): Promise<ConsumerGroupResult> {
    return this.withAdmin(async (admin) => {
      const described = await admin.describeGroups([groupId]);
      const group = requireLiveGroup(described.groups, groupId);
      const members = [...group.members].sort((a, b) => a.memberId.localeCompare(b.memberId));
      const page = members.slice(0, MAX_MEMBER_ROWS);
      return {
        groupId: group.groupId,
        state: group.state,
        protocolType: group.protocolType,
        protocol: group.protocol,
        memberCount: members.length,
        members: page.map((member) => ({
          memberId: member.memberId,
          clientId: member.clientId,
          clientHost: member.clientHost,
        })),
        truncated: members.length > page.length,
        observedAt: new Date().toISOString(),
      };
    });
  }

  async getConsumerAssignments(groupId: string): Promise<ConsumerAssignmentsResult> {
    return this.withAdmin(async (admin) => {
      const described = await admin.describeGroups([groupId]);
      const group = requireLiveGroup(described.groups, groupId);
      const members = [...group.members].sort((a, b) => a.memberId.localeCompare(b.memberId));
      const page = members.slice(0, MAX_MEMBER_ROWS);
      return {
        groupId: group.groupId,
        memberCount: members.length,
        assignments: page.map((member) => ({
          memberId: member.memberId,
          assignment: decodeAssignment(member.memberAssignment),
        })),
        truncated: members.length > page.length,
        observedAt: new Date().toISOString(),
      };
    });
  }

  private async withAdmin<T>(fn: (admin: Admin) => Promise<T>): Promise<T> {
    const admin = this.kafka.admin();
    let connected = false;
    try {
      await admin.connect();
      connected = true;
      return await fn(admin);
    } catch (err) {
      throw mapKafkaError(err);
    } finally {
      if (connected) {
        await admin.disconnect().catch(() => undefined);
      }
    }
  }
}

function requireLiveGroup(groups: GroupDescription[], groupId: string): GroupDescription {
  const group = groups.find((entry) => entry.groupId === groupId);
  // Observed live-broker behavior (Kafka 3.7): an unknown group is returned
  // with error 0 and state 'Dead' instead of raising GROUP_ID_NOT_FOUND.
  // A group with committed offsets but no active members reports 'Empty',
  // so 'Dead' means the group does not exist.
  if (group === undefined || group.state === 'Dead') {
    throw new AppError('NOT_FOUND', `Consumer group "${groupId}" not found`, {
      groupId,
      state: group?.state ?? 'Unknown',
    });
  }
  return group;
}

function decodeAssignment(buffer: Buffer | null | undefined): TopicAssignment[] | null {
  if (buffer == null || buffer.length === 0) {
    return null;
  }
  try {
    const decoded = AssignerProtocol.MemberAssignment.decode(buffer);
    if (decoded == null) {
      return null;
    }
    return Object.entries(decoded.assignment)
      .map(([topic, partitions]) => ({ topic, partitions: [...partitions].sort((a, b) => a - b) }))
      .sort((a, b) => a.topic.localeCompare(b.topic));
  } catch {
    return null;
  }
}
