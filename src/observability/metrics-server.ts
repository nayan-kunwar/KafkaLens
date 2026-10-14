import { createServer, type Server } from 'node:http';

import type { Logger } from 'pino';

import type { Metrics } from './metrics.js';

export function startMetricsServer(
  port: number,
  metrics: Metrics,
  logger: Logger,
): Promise<Server | null> {
  if (!metrics.enabled) return Promise.resolve(null);
  const server = createServer((request, response) => {
    if (request.method === 'GET' && request.url?.split('?')[0] === '/metrics') {
      metrics.render().then(
        (body) => {
          response.writeHead(200, { 'content-type': metrics.contentType });
          response.end(body);
        },
        () => {
          response.writeHead(500);
          response.end();
        },
      );
      return;
    }
    response.writeHead(404);
    response.end();
  });
  return new Promise((resolve, reject) => {
    const onError = (err: Error) => {
      logger.warn({ err: err.message, port }, 'metrics server failed to listen');
      reject(err);
    };
    server.once('error', onError);
    server.listen(port, '0.0.0.0', () => {
      server.removeListener('error', onError);
      logger.info({ port }, 'metrics server listening');
      resolve(server);
    });
  });
}
