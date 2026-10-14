import { describe, expect, it } from 'vitest';

import {
  analyzePartitionImbalancePrompt,
  diagnoseKafkaLagPrompt,
  explainConsumerGroupPrompt,
  investigateConsumerGroupPrompt,
} from '../../src/server/prompts.js';

describe('investigation prompts', () => {
  const prompts = [
    diagnoseKafkaLagPrompt('payments', 'payment-events'),
    investigateConsumerGroupPrompt('payments', 'payment-events'),
    analyzePartitionImbalancePrompt('payments', 'payment-events'),
    explainConsumerGroupPrompt('payments'),
    explainConsumerGroupPrompt('payments', 'payment-events'),
  ];

  it('names the tool to call and refuses a preset cause', () => {
    expect(prompts[0]).toContain('diagnose_consumer_group');
    expect(prompts[0]).toContain('payments');
    expect(prompts[2]).toContain('analyze_partition_imbalance');
    expect(prompts[3]).toContain('get_consumer_group');
    expect(prompts[3]).not.toContain('payment-events');
    expect(prompts[4]).toContain('payment-events');
    for (const prompt of prompts) {
      expect(prompt).toContain('observed_fact');
      expect(prompt).toContain('Do not decide');
      expect(prompt).not.toMatch(/the consumer is (slow|stalled|crashed)/i);
      expect(prompt).toContain('Do not reset offsets');
    }
  });
});
