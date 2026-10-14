import type { CallToolResult } from '@modelcontextprotocol/sdk/types.js';
import type { Logger } from 'pino';

import { noopMetrics, type Metrics } from '../observability/metrics.js';
import type {
  GetConsumerAssignmentsInput,
  GetConsumerGroupInput,
  ListConsumerGroupsInput,
} from '../schemas/consumer-group.js';
import type { ConsumerGroupsService } from '../services/consumer-groups-service.js';

import { executeTool } from './run-tool.js';

export function createListConsumerGroupsHandler(
  consumerGroupsService: ConsumerGroupsService,
  logger: Logger,
  metrics: Metrics = noopMetrics,
): (args: ListConsumerGroupsInput) => Promise<CallToolResult> {
  return (args) =>
    executeTool(
      logger,
      'list_consumer_groups',
      () => consumerGroupsService.listConsumerGroups(args),
      metrics,
    );
}

export function createGetConsumerGroupHandler(
  consumerGroupsService: ConsumerGroupsService,
  logger: Logger,
  metrics: Metrics = noopMetrics,
): (args: GetConsumerGroupInput) => Promise<CallToolResult> {
  return (args) =>
    executeTool(
      logger,
      'get_consumer_group',
      () => consumerGroupsService.getConsumerGroup(args.groupId),
      metrics,
    );
}

export function createGetConsumerAssignmentsHandler(
  consumerGroupsService: ConsumerGroupsService,
  logger: Logger,
  metrics: Metrics = noopMetrics,
): (args: GetConsumerAssignmentsInput) => Promise<CallToolResult> {
  return (args) =>
    executeTool(
      logger,
      'get_consumer_assignments',
      () => consumerGroupsService.getConsumerAssignments(args.groupId),
      metrics,
    );
}
