import type { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { z } from 'zod';

const groupId = z.string().min(1).max(255);
const topic = z.string().min(1).max(249);

const groupAndTopic = { groupId, topic };

function userPrompt(text: string): {
  messages: [{ role: 'user'; content: { type: 'text'; text: string } }];
} {
  return { messages: [{ role: 'user', content: { type: 'text', text } }] };
}

const EVIDENCE_RULES = `Separate every statement you make into one of these categories:
- observed_fact: a value read from Kafka or from stored history
- derived_metric: a number computed from those values; rates and shares are estimates when the tool says so
- inference: a deterministic classification from kafka-lens; it is not proof of a root cause
- recommendation: a next investigation step, not an action to execute

Do not decide that a consumer crashed, that a partition is hot, or that a producer is too fast until a finding cites evidence. Unknown lag is not zero. If a finding type is UNKNOWN, or history is unavailable, say what evidence is missing. Do not reset offsets, delete topics, produce messages, or stop consumers.`;

export function diagnoseKafkaLagPrompt(groupIdValue: string, topicValue: string): string {
  return `Investigate consumer lag for group "${groupIdValue}" on topic "${topicValue}".

Call diagnose_consumer_group with that group and topic. Use get_consumer_lag and get_lag_history only if you need a row the diagnosis report does not show.

${EVIDENCE_RULES}`;
}

export function investigateConsumerGroupPrompt(groupIdValue: string, topicValue: string): string {
  return `Investigate consumer group "${groupIdValue}" for topic "${topicValue}".

Start with diagnose_consumer_group. If the report marks assignments, history, incidents, or correlation as partial or unavailable, say so. You can call get_consumer_group, get_consumer_assignments, get_topic_metadata, and get_partition_info to inspect a specific fact.

${EVIDENCE_RULES}`;
}

export function analyzePartitionImbalancePrompt(groupIdValue: string, topicValue: string): string {
  return `Check whether lag for group "${groupIdValue}" on topic "${topicValue}" is uneven across partitions.

Call analyze_partition_imbalance. The threshold is PARTITION_IMBALANCE_RATIO. A result with insufficient evidence means the lag sample cannot support a skew claim. Do not describe a partition as hot unless it appears in outlierPartitionIds.

${EVIDENCE_RULES}`;
}

export function explainConsumerGroupPrompt(groupIdValue: string, topicValue?: string): string {
  const topicLine =
    topicValue === undefined
      ? 'No topic was provided. Call get_consumer_group and get_consumer_assignments. Ask for a topic before calling diagnose_consumer_group.'
      : `A topic was provided: "${topicValue}". After the group description, call diagnose_consumer_group for that topic.`;
  return `Explain the current state of consumer group "${groupIdValue}" without choosing a cause up front.

${topicLine}

Describe membership, protocol, and assignments as observed facts. Describe lag, rates, and findings only from tool output.

${EVIDENCE_RULES}`;
}

export function registerPrompts(server: McpServer): void {
  server.registerPrompt(
    'diagnose_kafka_lag',
    {
      title: 'Diagnose Kafka lag',
      description:
        'Asks for an evidence-based lag investigation. The prompt does not state a cause.',
      argsSchema: groupAndTopic,
    },
    (args) => userPrompt(diagnoseKafkaLagPrompt(args.groupId, args.topic)),
  );

  server.registerPrompt(
    'investigate_consumer_group',
    {
      title: 'Investigate consumer group',
      description:
        'Walks through group state, assignments, lag, and diagnosis without a preset conclusion.',
      argsSchema: groupAndTopic,
    },
    (args) => userPrompt(investigateConsumerGroupPrompt(args.groupId, args.topic)),
  );

  server.registerPrompt(
    'analyze_partition_imbalance',
    {
      title: 'Analyze partition imbalance',
      description:
        'Asks for partition-lag skew using the configured ratio. The prompt does not assume skew.',
      argsSchema: groupAndTopic,
    },
    (args) => userPrompt(analyzePartitionImbalancePrompt(args.groupId, args.topic)),
  );

  server.registerPrompt(
    'explain_consumer_group',
    {
      title: 'Explain consumer group',
      description: 'Asks for a description of one consumer group. Topic is optional.',
      argsSchema: { groupId, topic: topic.optional() },
    },
    (args) => userPrompt(explainConsumerGroupPrompt(args.groupId, args.topic)),
  );
}
