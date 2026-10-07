import { pino, type Logger } from 'pino';

export function createLogger(level: string): Logger {
  // stdout carries the MCP protocol; logs must go to stderr.
  return pino(
    {
      level,
      base: { service: 'kafka-lens' },
      timestamp: pino.stdTimeFunctions.isoTime,
    },
    pino.destination(2),
  );
}
