import { Counter, Histogram, Registry, type PrometheusContentType } from '@prometheus-io/client';

const PROMETHEUS_CONTENT_TYPE: PrometheusContentType = 'text/plain; version=0.0.4; charset=utf-8';

export interface Metrics {
  readonly enabled: boolean;
  readonly contentType: string;
  toolCall(tool: string, durationMs: number, errorCode?: string): void;
  kafkaRequest(operation: string, durationMs: number): void;
  diagnosis(operation: string, durationMs: number): void;
  lagObservations(count: number): void;
  render(): Promise<string>;
}

export const noopMetrics: Metrics = {
  enabled: false,
  contentType: PROMETHEUS_CONTENT_TYPE,
  toolCall() {
    return undefined;
  },
  kafkaRequest() {
    return undefined;
  },
  diagnosis() {
    return undefined;
  },
  lagObservations() {
    return undefined;
  },
  render() {
    return Promise.resolve('');
  },
};

export function createMetrics(enabled: boolean): Metrics {
  if (!enabled) return noopMetrics;
  return new PrometheusMetrics();
}

class PrometheusMetrics implements Metrics {
  readonly enabled = true;
  readonly contentType = PROMETHEUS_CONTENT_TYPE;
  private readonly registry = new Registry(PROMETHEUS_CONTENT_TYPE);
  private readonly toolCalls: Counter<'tool'>;
  private readonly toolErrors: Counter<'tool' | 'code'>;
  private readonly toolDuration: Histogram<'tool'>;
  private readonly kafkaDuration: Histogram<'operation'>;
  private readonly diagnosisDuration: Histogram<'operation'>;
  private readonly lagObservationsCounter: Counter<string>;

  constructor() {
    this.toolCalls = new Counter({
      name: 'mcp_tool_calls_total',
      help: 'MCP tool calls',
      labelNames: ['tool'],
      registers: [this.registry],
    });
    this.toolErrors = new Counter({
      name: 'mcp_tool_errors_total',
      help: 'MCP tool calls that returned an error',
      labelNames: ['tool', 'code'],
      registers: [this.registry],
    });
    this.toolDuration = new Histogram({
      name: 'mcp_tool_duration_seconds',
      help: 'MCP tool execution time',
      labelNames: ['tool'],
      buckets: [0.01, 0.05, 0.1, 0.3, 1, 3, 10],
      registers: [this.registry],
    });
    this.kafkaDuration = new Histogram({
      name: 'kafka_request_duration_seconds',
      help: 'Kafka admin request time by operation',
      labelNames: ['operation'],
      buckets: [0.01, 0.05, 0.1, 0.3, 1, 3, 10],
      registers: [this.registry],
    });
    this.diagnosisDuration = new Histogram({
      name: 'diagnosis_duration_seconds',
      help: 'Diagnosis operation time',
      labelNames: ['operation'],
      buckets: [0.01, 0.05, 0.1, 0.3, 1, 3, 10, 30],
      registers: [this.registry],
    });
    this.lagObservationsCounter = new Counter({
      name: 'lag_observations_total',
      help: 'Lag observations written to PostgreSQL',
      registers: [this.registry],
    });
  }

  toolCall(tool: string, durationMs: number, errorCode?: string): void {
    this.toolCalls.inc({ tool });
    this.toolDuration.observe({ tool }, seconds(durationMs));
    if (errorCode !== undefined) {
      this.toolErrors.inc({ tool, code: errorCode });
    }
  }

  kafkaRequest(operation: string, durationMs: number): void {
    this.kafkaDuration.observe({ operation }, seconds(durationMs));
  }

  diagnosis(operation: string, durationMs: number): void {
    this.diagnosisDuration.observe({ operation }, seconds(durationMs));
  }

  lagObservations(count: number): void {
    if (count > 0) this.lagObservationsCounter.inc(count);
  }

  render(): Promise<string> {
    return this.registry.metrics();
  }
}

function seconds(durationMs: number): number {
  return durationMs / 1000;
}
