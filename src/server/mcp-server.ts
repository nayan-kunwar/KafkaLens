import { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import type { Logger } from 'pino';

import { getPartitionInfoShape, getTopicMetadataShape, listTopicsShape } from '../schemas/topic.js';
import type { AdminService } from '../services/admin-service.js';
import { createGetClusterInfoHandler } from '../tools/cluster-info.js';
import {
  createGetPartitionInfoHandler,
  createGetTopicMetadataHandler,
  createListTopicsHandler,
} from '../tools/topics.js';

export const SERVER_INFO = { name: 'kafka-lens', version: '0.1.0' } as const;

export interface ServerDeps {
  adminService: AdminService;
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

  return server;
}
