import { Client } from '@modelcontextprotocol/sdk/client/index.js';
import { InMemoryTransport } from '@modelcontextprotocol/sdk/inMemory.js';
import type { Kafka } from 'kafkajs';
import { Pool } from 'pg';
import { pino } from 'pino';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import type { IncidentStore } from '../src/persistence/incident-store.js';
import { LagStore } from '../src/persistence/lag-store.js';
import { assertSafetyBoundary, READ_ONLY_TOOL_NAMES } from '../src/safety/readonly.js';
import { createMcpServer, SERVER_INFO } from '../src/server/mcp-server.js';
import { AdminService } from '../src/services/admin-service.js';
import { ConsumerGroupsService } from '../src/services/consumer-groups-service.js';
import type { DiagnosisService } from '../src/services/diagnosis-service.js';
import { OffsetsService } from '../src/services/offsets-service.js';

describe('smoke', () => {
  const logger = pino({ level: 'silent' });
  const kafka = {} as Kafka;
  const consumerGroupsService = new ConsumerGroupsService(kafka);
  const server = createMcpServer({
    adminService: new AdminService(kafka),
    consumerGroupsService,
    offsetsService: new OffsetsService(kafka, consumerGroupsService),
    // Never queried here; the pool stays idle without opening connections.
    lagStore: new LagStore(new Pool({ connectionString: 'postgresql://localhost:1/unused' })),
    diagnosisService: {} as DiagnosisService,
    incidentStore: {} as IncidentStore,
    logger,
  });
  const client = new Client({ name: 'kafka-lens-test', version: '0.0.0' });

  beforeAll(async () => {
    const [clientTransport, serverTransport] = InMemoryTransport.createLinkedPair();
    await Promise.all([server.connect(serverTransport), client.connect(clientTransport)]);
  });

  afterAll(async () => {
    await client.close();
    await server.close();
  });

  it('identifies itself as kafka-lens', () => {
    expect(SERVER_INFO.name).toBe('kafka-lens');
    expect(SERVER_INFO.version).toBe('0.2.0');
  });

  it('registers the read-only tool allowlist', async () => {
    assertSafetyBoundary();
    const { tools } = await client.listTools();
    const names = tools.map((tool) => tool.name).sort();
    expect(names).toEqual([...READ_ONLY_TOOL_NAMES].sort());
    for (const tool of tools) {
      expect(tool.annotations?.readOnlyHint).toBe(true);
      expect(tool.annotations?.destructiveHint).toBe(false);
    }
  });

  it('registers resources and prompts', async () => {
    const { resources } = await client.listResources();
    expect(resources.map((resource) => resource.uri).sort()).toEqual([
      'kafka://consumer-groups',
      'kafka://topics',
    ]);
    const { resourceTemplates } = await client.listResourceTemplates();
    expect(resourceTemplates.map((template) => template.uriTemplate).sort()).toEqual([
      'kafka://consumer-group/{group}',
      'kafka://consumer-group/{group}/lag',
      'kafka://incidents/{id}',
      'kafka://topic/{topic}',
    ]);
    const { prompts } = await client.listPrompts();
    expect(prompts.map((prompt) => prompt.name).sort()).toEqual([
      'analyze_partition_imbalance',
      'diagnose_kafka_lag',
      'explain_consumer_group',
      'investigate_consumer_group',
    ]);
  });

  it('answers health_check without Kafka access', async () => {
    const result = await client.callTool({ name: 'health_check', arguments: {} });
    expect(result.isError).not.toBe(true);
    expect(result.content).toEqual([{ type: 'text', text: 'kafka-lens is running' }]);
  });
});
