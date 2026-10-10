import { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import type { Logger } from 'pino';

import {
  getConsumerAssignmentsShape,
  getConsumerGroupShape,
  listConsumerGroupsShape,
} from '../schemas/consumer-group.js';
import { getPartitionInfoShape, getTopicMetadataShape, listTopicsShape } from '../schemas/topic.js';
import type { AdminService } from '../services/admin-service.js';
import type { ConsumerGroupsService } from '../services/consumer-groups-service.js';
import { createGetClusterInfoHandler } from '../tools/cluster-info.js';
import {
  createGetConsumerAssignmentsHandler,
  createGetConsumerGroupHandler,
  createListConsumerGroupsHandler,
} from '../tools/consumer-groups.js';
import {
  createGetPartitionInfoHandler,
  createGetTopicMetadataHandler,
  createListTopicsHandler,
} from '../tools/topics.js';

export const SERVER_INFO = { name: 'kafka-lens', version: '0.1.0' } as const;

export interface ServerDeps {
  adminService: AdminService;
  consumerGroupsService: ConsumerGroupsService;
  logger: Logger;
}

export function createMcpServer(deps: ServerDeps): McpServer {
  const server = new McpServer(SERVER_INFO);

  server.registerTool(
    'health_check',
    {
      title: 'Health check',
      description: 'Returns server liveness. No Kafka access required.',
    },
    () => ({
      content: [{ type: 'text', text: 'kafka-lens is running' }],
    }),
  );

  server.registerTool(
    'get_cluster_info',
    {
      title: 'Get cluster info',
      description: 'Returns Kafka cluster identity and broker list. Read-only connectivity check.',
    },
    createGetClusterInfoHandler(deps.adminService, deps.logger),
  );

  server.registerTool(
    'list_topics',
    {
      title: 'List topics',
      description:
        'Lists topic names in the cluster. Optional nameContains (literal substring) filters the list; ' +
        'limit caps the result (default 100, max 500). Response includes total and truncated flags. Read-only.',
      inputSchema: listTopicsShape,
    },
    createListTopicsHandler(deps.adminService, deps.logger),
  );

  server.registerTool(
    'get_topic_metadata',
    {
      title: 'Get topic metadata',
      description:
        'Returns partition layout for one topic: partition count and per-partition leader, replicas, and ISR. ' +
        'Errors with NOT_FOUND if the topic does not exist. Read-only.',
      inputSchema: getTopicMetadataShape,
    },
    createGetTopicMetadataHandler(deps.adminService, deps.logger),
  );

  server.registerTool(
    'get_partition_info',
    {
      title: 'Get partition info',
      description:
        'Returns per-partition detail for one topic: leader, replicas, ISR, and high/low watermarks ' +
        '(decimal strings; null means unknown). Optional partition returns that partition only, ' +
        'otherwise up to 500 partitions with partitionCount and truncated flags. Read-only.',
      inputSchema: getPartitionInfoShape,
    },
    createGetPartitionInfoHandler(deps.adminService, deps.logger),
  );

  server.registerTool(
    'list_consumer_groups',
    {
      title: 'List consumer groups',
      description:
        'Lists consumer group ids in the cluster. Optional groupIdContains (literal substring) ' +
        'and protocolType (exact match) filter the list; limit caps the result (default 100, max 500). ' +
        'Response includes state and memberCount per group with total and truncated flags. Read-only.',
      inputSchema: listConsumerGroupsShape,
    },
    createListConsumerGroupsHandler(deps.consumerGroupsService, deps.logger),
  );

  server.registerTool(
    'get_consumer_group',
    {
      title: 'Get consumer group',
      description:
        'Returns state, protocol, and members for one consumer group. ' +
        'Errors with NOT_FOUND if the group does not exist. Read-only.',
      inputSchema: getConsumerGroupShape,
    },
    createGetConsumerGroupHandler(deps.consumerGroupsService, deps.logger),
  );

  server.registerTool(
    'get_consumer_assignments',
    {
      title: 'Get consumer assignments',
      description:
        'Returns decoded topic-partition assignments per member for one consumer group. ' +
        'A null assignment means unknown (missing or undecodable buffer), not empty. ' +
        'Errors with NOT_FOUND if the group does not exist. Read-only.',
      inputSchema: getConsumerAssignmentsShape,
    },
    createGetConsumerAssignmentsHandler(deps.consumerGroupsService, deps.logger),
  );

  return server;
}
