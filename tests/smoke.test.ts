import { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { describe, expect, it } from 'vitest';

import { createMcpServer, SERVER_INFO } from '../src/server/mcp-server.js';

describe('smoke', () => {
  it('creates an MCP server with the expected identity', () => {
    const server = createMcpServer();
    expect(server).toBeInstanceOf(McpServer);
    expect(SERVER_INFO.name).toBe('kafka-lens');
  });
});
