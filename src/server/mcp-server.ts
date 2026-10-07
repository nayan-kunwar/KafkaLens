import { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import type { Logger } from 'pino';

import type { AdminService } from '../services/admin-service.js';
import { createGetClusterInfoHandler } from '../tools/cluster-info.js';

export const SERVER_INFO = { name: 'kafka-lens', version: '0.1.0' } as const;

export interface ServerDeps {
  adminService: AdminService;
  logger: Logger;
}

export function createMcpServer(deps: ServerDeps): McpServer {
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

  server.registerTool(
    'get_cluster_info',
    {
      title: 'Get cluster info',
      description: 'Returns Kafka cluster identity and broker list. Read-only connectivity check.',
    },
    createGetClusterInfoHandler(deps.adminService, deps.logger),
  );

  return server;
}
