import type { CallToolResult } from '@modelcontextprotocol/sdk/types.js';
import type { Logger } from 'pino';

import { noopMetrics, type Metrics } from '../observability/metrics.js';
import type {
  GetPartitionInfoInput,
  GetTopicMetadataInput,
  ListTopicsInput,
} from '../schemas/topic.js';
import type { AdminService } from '../services/admin-service.js';

import { executeTool } from './run-tool.js';

export function createListTopicsHandler(
  adminService: AdminService,
  logger: Logger,
  metrics: Metrics = noopMetrics,
): (args: ListTopicsInput) => Promise<CallToolResult> {
  return (args) => executeTool(logger, 'list_topics', () => adminService.listTopics(args), metrics);
}

export function createGetTopicMetadataHandler(
  adminService: AdminService,
  logger: Logger,
  metrics: Metrics = noopMetrics,
): (args: GetTopicMetadataInput) => Promise<CallToolResult> {
  return (args) =>
    executeTool(
      logger,
      'get_topic_metadata',
      () => adminService.getTopicMetadata(args.topic),
      metrics,
    );
}

export function createGetPartitionInfoHandler(
  adminService: AdminService,
  logger: Logger,
  metrics: Metrics = noopMetrics,
): (args: GetPartitionInfoInput) => Promise<CallToolResult> {
  return (args) =>
    executeTool(
      logger,
      'get_partition_info',
      () => adminService.getPartitionInfo(args.topic, args.partition),
      metrics,
    );
}
