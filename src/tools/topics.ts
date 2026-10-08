import type { CallToolResult } from '@modelcontextprotocol/sdk/types.js';
import type { Logger } from 'pino';

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
): (args: ListTopicsInput) => Promise<CallToolResult> {
  return (args) => executeTool(logger, 'list_topics', () => adminService.listTopics(args));
}

export function createGetTopicMetadataHandler(
  adminService: AdminService,
  logger: Logger,
): (args: GetTopicMetadataInput) => Promise<CallToolResult> {
  return (args) =>
    executeTool(logger, 'get_topic_metadata', () => adminService.getTopicMetadata(args.topic));
}

export function createGetPartitionInfoHandler(
  adminService: AdminService,
  logger: Logger,
): (args: GetPartitionInfoInput) => Promise<CallToolResult> {
  return (args) =>
    executeTool(logger, 'get_partition_info', () =>
      adminService.getPartitionInfo(args.topic, args.partition),
    );
}
