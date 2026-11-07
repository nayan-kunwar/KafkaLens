import type { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import type { ReadResourceResult } from '@modelcontextprotocol/sdk/types.js';
import { describe, expect, it } from 'vitest';

import { AppError } from '../../src/errors/app-error.js';
import type { IncidentStore } from '../../src/persistence/incident-store.js';
import { registerResources, type ResourceDeps } from '../../src/server/resources.js';
import type { AdminService } from '../../src/services/admin-service.js';
import type { ConsumerGroupsService } from '../../src/services/consumer-groups-service.js';
import type { OffsetsService } from '../../src/services/offsets-service.js';

type ResourceCallback = (
  uri: URL,
  variables: Record<string, string | string[]>,
) => Promise<ReadResourceResult>;

function setup(): { server: McpServer; callbacks: Map<string, ResourceCallback> } {
  const callbacks = new Map<string, ResourceCallback>();
  const server = {
    registerResource: (
      name: string,
      _target: unknown,
      _metadata: unknown,
      callback: ResourceCallback,
    ) => {
      callbacks.set(name, callback);
    },
  } as unknown as McpServer;
  return { server, callbacks };
}

function stubs(): { deps: ResourceDeps; calls: Record<string, number> } {
  const calls: Record<string, number> = {
    listTopics: 0,
    getTopicMetadata: 0,
    listConsumerGroups: 0,
    getConsumerGroup: 0,
    getConsumerLag: 0,
    getById: 0,
  };
  const observedAt = '2026-01-01T00:00:00.000Z';
  return {
    calls,
    deps: {
      adminService: {
        listTopics: () => {
          calls.listTopics = (calls.listTopics ?? 0) + 1;
          return Promise.resolve({ topics: [], total: 0, truncated: false, observedAt });
        },
        getTopicMetadata: (topic: string) => {
          calls.getTopicMetadata = (calls.getTopicMetadata ?? 0) + 1;
          return Promise.resolve({ name: topic, partitionCount: 1, partitions: [], observedAt });
        },
      } as unknown as AdminService,
      consumerGroupsService: {
        listConsumerGroups: () => {
          calls.listConsumerGroups = (calls.listConsumerGroups ?? 0) + 1;
          return Promise.resolve({ groups: [], total: 0, truncated: false, observedAt });
        },
        getConsumerGroup: (groupId: string) => {
          calls.getConsumerGroup = (calls.getConsumerGroup ?? 0) + 1;
          return Promise.resolve({
            groupId,
            state: 'Stable',
            protocolType: 'consumer',
            protocol: 'range',
            memberCount: 0,
            members: [],
            truncated: false,
            observedAt,
          });
        },
      } as unknown as ConsumerGroupsService,
      offsetsService: {
        getConsumerLag: (groupId: string) => {
          calls.getConsumerLag = (calls.getConsumerLag ?? 0) + 1;
          return Promise.resolve({
            groupId,
            groupState: 'Stable',
            topics: [],
            topicCount: 0,
            truncated: false,
            observedAt,
          });
        },
      } as unknown as OffsetsService,
      incidentStore: {
        getById: (id: string) => {
          calls.getById = (calls.getById ?? 0) + 1;
          return Promise.resolve({ id, summary: 'stored incident' });
        },
      } as unknown as IncidentStore,
    },
  };
}

function callbackOf(callbacks: Map<string, ResourceCallback>, name: string): ResourceCallback {
  const callback = callbacks.get(name);
  if (callback === undefined) throw new Error(`resource ${name} was not registered`);
  return callback;
}

function textOf(result: ReadResourceResult): string {
  const content = result.contents[0];
  if (content === undefined || !('text' in content)) {
    throw new Error('expected a text resource');
  }
  return content.text;
}

describe('registerResources', () => {
  it('registers every documented resource', () => {
    const { server, callbacks } = setup();
    registerResources(server, stubs().deps);
    expect([...callbacks.keys()].sort()).toEqual([
      'consumer-group',
      'consumer-group-lag',
      'consumer-groups',
      'incident',
      'topic',
      'topics',
    ]);
  });

  it('rejects an invalid topic name before touching Kafka', async () => {
    const { server, callbacks } = setup();
    const { deps, calls } = stubs();
    registerResources(server, deps);

    const thrown = await callbackOf(callbacks, 'topic')(new URL('kafka://topic/bad%20name'), {
      topic: 'bad name',
    }).catch((err: unknown) => err);

    expect(thrown).toBeInstanceOf(AppError);
    expect((thrown as AppError).code).toBe('NOT_FOUND');
    expect(calls.getTopicMetadata).toBe(0);
  });

  it('returns compact JSON for a valid topic and decodes the variable', async () => {
    const { server, callbacks } = setup();
    const { deps, calls } = stubs();
    registerResources(server, deps);

    const result = await callbackOf(callbacks, 'topic')(new URL('kafka://topic/orders'), {
      topic: 'orders',
    });
    expect(calls.getTopicMetadata).toBe(1);
    expect(textOf(result)).toBe(
      '{"name":"orders","partitionCount":1,"partitions":[],"observedAt":"2026-01-01T00:00:00.000Z"}',
    );
    expect(textOf(result)).not.toContain('\n');
  });

  it('rejects a non-numeric incident id', async () => {
    const { server, callbacks } = setup();
    const { deps, calls } = stubs();
    registerResources(server, deps);

    const thrown = await callbackOf(callbacks, 'incident')(new URL('kafka://incidents/1x'), {
      id: '1x',
    }).catch((err: unknown) => err);

    expect(thrown).toBeInstanceOf(AppError);
    expect((thrown as AppError).code).toBe('NOT_FOUND');
    expect(calls.getById).toBe(0);
  });

  it('rejects a missing variable and accepts a numeric incident id', async () => {
    const { server, callbacks } = setup();
    const { deps, calls } = stubs();
    registerResources(server, deps);

    await expect(
      callbackOf(callbacks, 'consumer-group')(new URL('kafka://consumer-group/'), {}),
    ).rejects.toMatchObject({ code: 'NOT_FOUND' });

    const result = await callbackOf(callbacks, 'incident')(new URL('kafka://incidents/12'), {
      id: '12',
    });
    expect(calls.getById).toBe(1);
    expect(textOf(result)).toContain('"id":"12"');
  });
});
