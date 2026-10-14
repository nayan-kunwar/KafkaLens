import type { CallToolResult } from '@modelcontextprotocol/sdk/types.js';
import type { Logger } from 'pino';

import { noopMetrics, type Metrics } from '../observability/metrics.js';
import type {
  DiagnoseConsumerGroupInput,
  FindSimilarIncidentsInput,
} from '../schemas/diagnosis.js';
import type { DiagnosisService } from '../services/diagnosis-service.js';

import { executeTool } from './run-tool.js';

export function createDiagnoseConsumerGroupHandler(
  diagnosis: DiagnosisService,
  logger: Logger,
  metrics: Metrics = noopMetrics,
): (args: DiagnoseConsumerGroupInput) => Promise<CallToolResult> {
  return (args) =>
    executeTool(
      logger,
      'diagnose_consumer_group',
      () => diagnosis.diagnoseConsumerGroup(args.groupId, args.topic),
      metrics,
    );
}

export function createAnalyzeConsumerLagHandler(
  diagnosis: DiagnosisService,
  logger: Logger,
  metrics: Metrics = noopMetrics,
): (args: DiagnoseConsumerGroupInput) => Promise<CallToolResult> {
  return (args) =>
    executeTool(
      logger,
      'analyze_consumer_lag',
      () => diagnosis.analyzeConsumerLag(args.groupId, args.topic),
      metrics,
    );
}

export function createAnalyzePartitionImbalanceHandler(
  diagnosis: DiagnosisService,
  logger: Logger,
  metrics: Metrics = noopMetrics,
): (args: DiagnoseConsumerGroupInput) => Promise<CallToolResult> {
  return (args) =>
    executeTool(
      logger,
      'analyze_partition_imbalance',
      () => diagnosis.analyzePartitionImbalance(args.groupId, args.topic),
      metrics,
    );
}

export function createAnalyzeThroughputHandler(
  diagnosis: DiagnosisService,
  logger: Logger,
  metrics: Metrics = noopMetrics,
): (args: DiagnoseConsumerGroupInput) => Promise<CallToolResult> {
  return (args) =>
    executeTool(
      logger,
      'analyze_throughput',
      () => diagnosis.analyzeThroughput(args.groupId, args.topic),
      metrics,
    );
}

export function createDetectStalledConsumersHandler(
  diagnosis: DiagnosisService,
  logger: Logger,
  metrics: Metrics = noopMetrics,
): (args: DiagnoseConsumerGroupInput) => Promise<CallToolResult> {
  return (args) =>
    executeTool(
      logger,
      'detect_stalled_consumers',
      () => diagnosis.detectStalledConsumers(args.groupId, args.topic),
      metrics,
    );
}

export function createFindSimilarIncidentsHandler(
  diagnosis: DiagnosisService,
  logger: Logger,
  metrics: Metrics = noopMetrics,
): (args: FindSimilarIncidentsInput) => Promise<CallToolResult> {
  return (args) =>
    executeTool(
      logger,
      'find_similar_incidents',
      () =>
        diagnosis.findSimilarIncidents({
          groupId: args.groupId,
          topic: args.topic,
          findingTypes: args.findingTypes ?? [],
          limit: args.limit,
        }),
      metrics,
    );
}
