import { z } from 'zod';

import { AppError } from '../errors/app-error.js';

const envSchema = z.object({
  NODE_ENV: z.enum(['development', 'test', 'production']).default('development'),
  KAFKA_BROKERS: z.string().min(1).default('localhost:9092'),
  KAFKA_CLIENT_ID: z.string().min(1).default('kafka-inspector'),
  KAFKA_SECURITY_PROTOCOL: z
    .enum(['PLAINTEXT', 'SSL', 'SASL_PLAINTEXT', 'SASL_SSL'])
    .default('PLAINTEXT'),
  POSTGRES_URL: z
    .string()
    .min(1)
    .default('postgresql://postgres:postgres@localhost:5432/kafka_inspector'),
  REDIS_URL: z.string().min(1).default('redis://localhost:6379'),
  LOG_LEVEL: z.enum(['fatal', 'error', 'warn', 'info', 'debug', 'trace']).default('info'),
  PARTITION_IMBALANCE_RATIO: z.coerce.number().positive().default(5),
  STALL_WINDOW_SECONDS: z.coerce.number().positive().default(300),
});

export type Env = z.infer<typeof envSchema>;

export function loadEnv(source: NodeJS.ProcessEnv = process.env): Env {
  const parsed = envSchema.safeParse(source);
  if (!parsed.success) {
    // Field names and codes only — never raw values (they may contain credentials).
    const issues = parsed.error.issues.map(
      (issue) => `${issue.path.join('.') || 'env'} (${issue.code})`,
    );
    throw new AppError(
      'CONFIG_INVALID',
      `Invalid environment configuration: ${issues.join(', ')}`,
      {
        fields: issues,
      },
    );
  }
  return parsed.data;
}
