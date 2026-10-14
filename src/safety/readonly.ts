import { AppError } from '../errors/app-error.js';

export const READ_ONLY_TOOL_NAMES = [
  'health_check',
  'get_cluster_info',
  'list_topics',
  'get_topic_metadata',
  'get_partition_info',
  'list_consumer_groups',
  'get_consumer_group',
  'get_consumer_assignments',
  'get_consumer_lag',
  'get_lag_history',
  'analyze_consumer_lag',
  'analyze_partition_imbalance',
  'analyze_throughput',
  'detect_stalled_consumers',
  'diagnose_consumer_group',
  'find_similar_incidents',
] as const;

export type ReadOnlyToolName = (typeof READ_ONLY_TOOL_NAMES)[number];

/** Kafka mutations this server must never perform. */
export const FORBIDDEN_KAFKA_OPERATIONS = [
  'delete_topic',
  'reset_offsets',
  'alter_partitions',
  'produce_message',
  'stop_consumer',
  'delete_consumer_group',
  'alter_config',
] as const;

const ALLOWED = new Set<string>(READ_ONLY_TOOL_NAMES);
const FORBIDDEN = new Set<string>(FORBIDDEN_KAFKA_OPERATIONS);

export function assertReadOnlyTool(name: string): asserts name is ReadOnlyToolName {
  if (FORBIDDEN.has(name) || !ALLOWED.has(name)) {
    throw new AppError(
      'SAFETY_REJECTED',
      `Refusing "${name}": kafka-lens only exposes read-only investigation tools.`,
      { operation: name },
    );
  }
}

/**
 * Gate for any future Kafka write. It always rejects.
 * A write, if one is ever added, also needs an explicit user confirmation
 * outside this function. Model output alone is not confirmation.
 */
export function rejectKafkaMutation(operation: string): never {
  throw new AppError(
    'SAFETY_REJECTED',
    `Refusing Kafka mutation "${operation}". kafka-lens does not execute destructive operations.`,
    { operation },
  );
}

/** Fails startup if the allowlist contains a mutation or fails to reject one. */
export function assertSafetyBoundary(): void {
  for (const name of READ_ONLY_TOOL_NAMES) {
    assertReadOnlyTool(name);
  }
  for (const name of FORBIDDEN_KAFKA_OPERATIONS) {
    let rejected = false;
    try {
      assertReadOnlyTool(name);
    } catch (err) {
      rejected = err instanceof AppError && err.code === 'SAFETY_REJECTED';
    }
    if (!rejected) {
      throw new AppError('SAFETY_REJECTED', `Safety boundary failed to reject "${name}"`);
    }
  }
}
