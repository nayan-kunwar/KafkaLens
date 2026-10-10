import { Client } from '@modelcontextprotocol/sdk/client/index.js';
import { InMemoryTransport } from '@modelcontextprotocol/sdk/inMemory.js';
import type { Kafka } from 'kafkajs';
import { pino } from 'pino';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import { createMcpServer, SERVER_INFO } from '../src/server/mcp-server.js';
import { AdminService } from '../src/services/admin-service.js';
import { ConsumerGroupsService } from '../src/services/consumer-groups-service.js';

describe('smoke', () => {
  const logger = pino({ level: 'silent' });
  const server = createMcpServer({
    adminService: new AdminService({} as Kafka),
    consumerGroupsService: new ConsumerGroupsService({} as Kafka),
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
  });

  it('registers the expected tools', async () => {
    const { tools } = await client.listTools();
    const names = tools.map((tool) => tool.name);
    expect(names).toEqual(
      expect.arrayContaining([
        'health_check',
        'get_cluster_info',
        'list_topics',
        'get_topic_metadata',
        'get_partition_info',
        'list_consumer_groups',
        'get_consumer_group',
        'get_consumer_assignments',
      ]),
    );
  });

  it('answers health_check without Kafka access', async () => {
    const result = await client.callTool({ name: 'health_check', arguments: {} });
    expect(result.isError).not.toBe(true);
    expect(result.content).toEqual([{ type: 'text', text: 'kafka-lens is running' }]);
  });
});
