import { describe, expect, it } from 'vitest';

import { createMetrics, noopMetrics } from '../../src/observability/metrics.js';

describe('metrics', () => {
  it('renders the diagnosis counters and histograms when enabled', async () => {
    const metrics = createMetrics(true);
    metrics.toolCall('health_check', 12);
    metrics.toolCall('get_cluster_info', 40, 'NOT_FOUND');
    metrics.kafkaRequest('describe_cluster', 20);
    metrics.diagnosis('diagnose_consumer_group', 80);
    metrics.lagObservations(4);
    const text = await metrics.render();
    expect(text).toContain('mcp_tool_calls_total');
    expect(text).toContain('mcp_tool_errors_total');
    expect(text).toContain('kafka_request_duration_seconds');
    expect(text).toContain('diagnosis_duration_seconds');
    expect(text).toContain('lag_observations_total');
  });

  it('leaves the noop recorder silent', async () => {
    noopMetrics.toolCall('health_check', 1, 'INTERNAL');
    noopMetrics.lagObservations(2);
    expect(noopMetrics.enabled).toBe(false);
    expect(await noopMetrics.render()).toBe('');
  });
});
