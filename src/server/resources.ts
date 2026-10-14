import { ResourceTemplate, type McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import type { ReadResourceResult } from '@modelcontextprotocol/sdk/types.js';

import { AppError } from '../errors/app-error.js';
import type { IncidentStore } from '../persistence/incident-store.js';
import type { AdminService } from '../services/admin-service.js';
import type { ConsumerGroupsService } from '../services/consumer-groups-service.js';
import type { OffsetsService } from '../services/offsets-service.js';

export interface ResourceDeps {
  adminService: AdminService;
  consumerGroupsService: ConsumerGroupsService;
  offsetsService: OffsetsService;
  incidentStore: IncidentStore;
}

export function registerResources(server: McpServer, deps: ResourceDeps): void {
  server.registerResource(
    'topics',
    'kafka://topics',
    {
      title: 'Topics',
      description: 'Bounded topic list for the connected cluster.',
      mimeType: 'application/json',
    },
    (uri) => jsonResource(uri, () => deps.adminService.listTopics({ limit: 100 })),
  );

  server.registerResource(
    'topic',
    new ResourceTemplate('kafka://topic/{topic}', { list: undefined }),
    {
      title: 'Topic',
      description: 'Partition layout for one topic.',
      mimeType: 'application/json',
    },
    (uri, variables) =>
      jsonResource(uri, () => deps.adminService.getTopicMetadata(one(variables, 'topic'))),
  );

  server.registerResource(
    'consumer-groups',
    'kafka://consumer-groups',
    {
      title: 'Consumer groups',
      description: 'Bounded consumer group list.',
      mimeType: 'application/json',
    },
    (uri) => jsonResource(uri, () => deps.consumerGroupsService.listConsumerGroups({ limit: 100 })),
  );

  server.registerResource(
    'consumer-group',
    new ResourceTemplate('kafka://consumer-group/{group}', { list: undefined }),
    {
      title: 'Consumer group',
      description: 'State and members for one consumer group.',
      mimeType: 'application/json',
    },
    (uri, variables) =>
      jsonResource(uri, () => deps.consumerGroupsService.getConsumerGroup(one(variables, 'group'))),
  );

  server.registerResource(
    'consumer-group-lag',
    new ResourceTemplate('kafka://consumer-group/{group}/lag', { list: undefined }),
    {
      title: 'Consumer group lag',
      description: 'Per-topic lag summary for one consumer group.',
      mimeType: 'application/json',
    },
    (uri, variables) =>
      jsonResource(uri, () => deps.offsetsService.getConsumerLag(one(variables, 'group'))),
  );

  server.registerResource(
    'incident',
    new ResourceTemplate('kafka://incidents/{id}', { list: undefined }),
    {
      title: 'Incident',
      description: 'One stored diagnosis incident.',
      mimeType: 'application/json',
    },
    (uri, variables) => jsonResource(uri, () => deps.incidentStore.getById(one(variables, 'id'))),
  );
}

async function jsonResource(uri: URL, load: () => Promise<unknown>): Promise<ReadResourceResult> {
  try {
    const data = await load();
    return {
      contents: [
        { uri: uri.href, mimeType: 'application/json', text: JSON.stringify(data, null, 2) },
      ],
    };
  } catch (err) {
    const appError =
      err instanceof AppError ? err : new AppError('INTERNAL', 'Resource read failed');
    throw appError;
  }
}

function one(variables: Record<string, string | string[]>, name: string): string {
  const value = variables[name];
  const raw = Array.isArray(value) ? value[0] : value;
  if (raw === undefined || raw.length === 0) {
    throw new AppError('NOT_FOUND', `Resource is missing "${name}"`, { name });
  }
  try {
    return decodeURIComponent(raw);
  } catch {
    throw new AppError('NOT_FOUND', `Resource variable "${name}" is not valid`, { name });
  }
}
