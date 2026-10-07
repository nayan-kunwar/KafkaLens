import { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';

export const SERVER_INFO = { name: 'kafka-lens', version: '0.1.0' } as const;

export function createMcpServer(): McpServer {
  const server = new McpServer(SERVER_INFO);

  server.registerTool(
    'health_check',
    {
      title: 'Health check',
      description: 'Returns server liveness. No Kafka access required.',
    },
    () => ({
      content: [{ type: 'text', text: 'kafka-lens is running' }],
    }),
  );

  return server;
}
