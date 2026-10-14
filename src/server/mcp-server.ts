import { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import type { Logger } from 'pino';

import { noopMetrics, type Metrics } from '../observability/metrics.js';
import type { IncidentStore } from '../persistence/incident-store.js';
import type { LagStore } from '../persistence/lag-store.js';
import {
  assertReadOnlyTool,
  assertSafetyBoundary,
  type ReadOnlyToolName,
} from '../safety/readonly.js';
import {
  getConsumerAssignmentsShape,
  getConsumerGroupShape,
  listConsumerGroupsShape,
} from '../schemas/consumer-group.js';
import { diagnoseConsumerGroupShape, findSimilarIncidentsShape } from '../schemas/diagnosis.js';
import { getLagHistoryShape } from '../schemas/history.js';
import { getConsumerLagShape } from '../schemas/lag.js';
import { getPartitionInfoShape, getTopicMetadataShape, listTopicsShape } from '../schemas/topic.js';
import type { AdminService } from '../services/admin-service.js';
import type { ConsumerGroupsService } from '../services/consumer-groups-service.js';
import type { DiagnosisService } from '../services/diagnosis-service.js';
import type { OffsetsService } from '../services/offsets-service.js';
import { createGetClusterInfoHandler } from '../tools/cluster-info.js';
import {
  createGetConsumerAssignmentsHandler,
  createGetConsumerGroupHandler,
  createListConsumerGroupsHandler,
} from '../tools/consumer-groups.js';
import {
  createAnalyzeConsumerLagHandler,
  createAnalyzePartitionImbalanceHandler,
  createAnalyzeThroughputHandler,
  createDetectStalledConsumersHandler,
  createDiagnoseConsumerGroupHandler,
  createFindSimilarIncidentsHandler,
} from '../tools/diagnosis.js';
import { createGetLagHistoryHandler } from '../tools/history.js';
import { createGetConsumerLagHandler } from '../tools/offsets.js';
import {
  createGetPartitionInfoHandler,
  createGetTopicMetadataHandler,
  createListTopicsHandler,
} from '../tools/topics.js';

import { registerPrompts } from './prompts.js';
import { registerResources } from './resources.js';

export const SERVER_INFO = { name: 'kafka-lens', version: '0.2.0' } as const;

export interface ServerDeps {
  adminService: AdminService;
  consumerGroupsService: ConsumerGroupsService;
  offsetsService: OffsetsService;
  lagStore: LagStore;
  diagnosisService: DiagnosisService;
  incidentStore: IncidentStore;
  logger: Logger;
  metrics?: Metrics | undefined;
}

export function createMcpServer(deps: ServerDeps): McpServer {
  assertSafetyBoundary();
  const metrics = deps.metrics ?? noopMetrics;
  const server = new McpServer(SERVER_INFO);

  const register = (
    name: ReadOnlyToolName,
    config: { title: string; description: string; inputSchema?: object },
    handler: (args: never) => Promise<{ content: { type: 'text'; text: string }[] }>,
  ): void => {
    assertReadOnlyTool(name);
    const toolConfig = {
      title: config.title,
      description: config.description,
      annotations: {
        readOnlyHint: true,
        destructiveHint: false,
        idempotentHint: true,
        openWorldHint: name !== 'health_check',
      },
      ...(config.inputSchema === undefined ? {} : { inputSchema: config.inputSchema }),
    };
    server.registerTool(name, toolConfig as never, handler as never);
  };

  register(
    'health_check',
    {
      title: 'Health check',
      description: 'Returns server liveness. No Kafka access required.',
    },
    () => {
      const started = Date.now();
      metrics.toolCall('health_check', Date.now() - started);
      return Promise.resolve({
        content: [{ type: 'text' as const, text: 'kafka-lens is running' }],
      });
    },
  );

  register(
    'get_cluster_info',
    {
      title: 'Get cluster info',
      description: 'Returns Kafka cluster identity and broker list. Read-only connectivity check.',
    },
    createGetClusterInfoHandler(deps.adminService, deps.logger, metrics) as never,
  );

  register(
    'list_topics',
    {
      title: 'List topics',
      description:
        'Lists topic names in the cluster. Optional nameContains (literal substring) filters the list; ' +
        'limit caps the result (default 100, max 500). Response includes total and truncated flags. Read-only.',
      inputSchema: listTopicsShape,
    },
    createListTopicsHandler(deps.adminService, deps.logger, metrics) as never,
  );

  register(
    'get_topic_metadata',
    {
      title: 'Get topic metadata',
      description:
        'Returns partition layout for one topic: partition count and per-partition leader, replicas, and ISR. ' +
        'Errors with NOT_FOUND if the topic does not exist. Read-only.',
      inputSchema: getTopicMetadataShape,
    },
    createGetTopicMetadataHandler(deps.adminService, deps.logger, metrics) as never,
  );

  register(
    'get_partition_info',
    {
      title: 'Get partition info',
      description:
        'Returns per-partition detail for one topic: leader, replicas, ISR, and high/low watermarks ' +
        '(decimal strings; null means unknown). Optional partition returns that partition only, ' +
        'otherwise up to 500 partitions with partitionCount and truncated flags. Read-only.',
      inputSchema: getPartitionInfoShape,
    },
    createGetPartitionInfoHandler(deps.adminService, deps.logger, metrics) as never,
  );

  register(
    'list_consumer_groups',
    {
      title: 'List consumer groups',
      description:
        'Lists consumer group ids in the cluster. Optional groupIdContains (literal substring) ' +
        'and protocolType (exact match) filter the list; limit caps the result (default 100, max 500). ' +
        'Response includes state and memberCount per group with total and truncated flags. Read-only.',
      inputSchema: listConsumerGroupsShape,
    },
    createListConsumerGroupsHandler(deps.consumerGroupsService, deps.logger, metrics) as never,
  );

  register(
    'get_consumer_group',
    {
      title: 'Get consumer group',
      description:
        'Returns state, protocol, and members for one consumer group. ' +
        'Errors with NOT_FOUND if the group does not exist. Read-only.',
      inputSchema: getConsumerGroupShape,
    },
    createGetConsumerGroupHandler(deps.consumerGroupsService, deps.logger, metrics) as never,
  );

  register(
    'get_consumer_assignments',
    {
      title: 'Get consumer assignments',
      description:
        'Returns decoded topic-partition assignments per member for one consumer group. ' +
        'A null assignment means unknown (missing or undecodable buffer), not empty. ' +
        'Errors with NOT_FOUND if the group does not exist. Read-only.',
      inputSchema: getConsumerAssignmentsShape,
    },
    createGetConsumerAssignmentsHandler(deps.consumerGroupsService, deps.logger, metrics) as never,
  );

  register(
    'get_consumer_lag',
    {
      title: 'Get consumer lag',
      description:
        'Returns per-partition lag (high watermark minus committed offset) for one consumer group. ' +
        'With topic, returns one row per partition; without topic, one summary row per committed topic. ' +
        'Lag null means unknown (never zero-filled); totals sum known partitions only. ' +
        'Observations are also persisted to lag history when PostgreSQL is available. Read-only.',
      inputSchema: getConsumerLagShape,
    },
    createGetConsumerLagHandler(deps.offsetsService, deps.logger, metrics) as never,
  );

  register(
    'get_lag_history',
    {
      title: 'Get lag history',
      description:
        'Returns recorded lag observations for one consumer group and topic, newest first. ' +
        'Each row includes the partition id. A null lag means unknown at that time. ' +
        'Errors with STORE_UNAVAILABLE when PostgreSQL is unreachable. Read-only.',
      inputSchema: getLagHistoryShape,
    },
    createGetLagHistoryHandler(deps.lagStore, deps.logger, metrics) as never,
  );

  register(
    'analyze_consumer_lag',
    {
      title: 'Analyze consumer lag',
      description:
        'Reads live lag and stored history for one group and topic, then returns deterministic findings. ' +
        'Unknown lag stays null. History and assignment failures are reported and do not fail the call. Read-only.',
      inputSchema: diagnoseConsumerGroupShape,
    },
    createAnalyzeConsumerLagHandler(deps.diagnosisService, deps.logger, metrics) as never,
  );

  register(
    'analyze_partition_imbalance',
    {
      title: 'Analyze partition imbalance',
      description:
        'Compares known per-partition lag with PARTITION_IMBALANCE_RATIO. ' +
        'Fewer than two known lags is insufficient evidence. Read-only.',
      inputSchema: diagnoseConsumerGroupShape,
    },
    createAnalyzePartitionImbalanceHandler(deps.diagnosisService, deps.logger, metrics) as never,
  );

  register(
    'analyze_throughput',
    {
      title: 'Analyze throughput',
      description:
        'Estimates incoming rate, processing rate, and lag growth from stored offsets. ' +
        'Rates are labeled estimates and are not broker producer metrics. Read-only.',
      inputSchema: diagnoseConsumerGroupShape,
    },
    createAnalyzeThroughputHandler(deps.diagnosisService, deps.logger, metrics) as never,
  );

  register(
    'detect_stalled_consumers',
    {
      title: 'Detect stalled consumers',
      description:
        'Classifies each partition as not assigned, assigned but not progressing, progressing slowly, ' +
        'actively processing, or insufficient evidence. No progress is not reported as a crash. Read-only.',
      inputSchema: diagnoseConsumerGroupShape,
    },
    createDetectStalledConsumersHandler(deps.diagnosisService, deps.logger, metrics) as never,
  );

  register(
    'diagnose_consumer_group',
    {
      title: 'Diagnose consumer group',
      description:
        'Runs the investigation workflow for one group and topic: metadata, membership, assignments, ' +
        'live lag, lag history, imbalance, progress, throughput, and ranked findings with evidence. ' +
        'Optional history, incidents, and change correlation fail open. Read-only.',
      inputSchema: diagnoseConsumerGroupShape,
    },
    createDiagnoseConsumerGroupHandler(deps.diagnosisService, deps.logger, metrics) as never,
  );

  register(
    'find_similar_incidents',
    {
      title: 'Find similar incidents',
      description:
        'Returns stored diagnosis incidents that overlap the group, topic, or finding types. ' +
        'Scores are deterministic. Errors with STORE_UNAVAILABLE when PostgreSQL is unreachable. Read-only.',
      inputSchema: findSimilarIncidentsShape,
    },
    createFindSimilarIncidentsHandler(deps.diagnosisService, deps.logger, metrics) as never,
  );

  registerResources(server, deps);
  registerPrompts(server);
  return server;
}
