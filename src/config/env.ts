import { z } from 'zod';

import { AppError } from '../errors/app-error.js';

const boolFromEnv = z
  .string()
  .default('true')
  .transform((value, ctx) => {
    if (value === 'true') return true;
    if (value === 'false') return false;
    ctx.addIssue({ code: 'custom', message: 'must be "true" or "false"' });
    return z.NEVER;
  });

const envSchema = z
  .object({
    NODE_ENV: z.enum(['development', 'test', 'production']).default('development'),
    KAFKA_BROKERS: z.string().min(1).default('localhost:9092'),
    KAFKA_CLIENT_ID: z.string().min(1).default('kafka-inspector'),
    KAFKA_SECURITY_PROTOCOL: z
      .enum(['PLAINTEXT', 'SSL', 'SASL_PLAINTEXT', 'SASL_SSL'])
      .default('PLAINTEXT'),
    KAFKA_SASL_MECHANISM: z.enum(['plain', 'scram-sha-256', 'scram-sha-512']).default('plain'),
    KAFKA_SASL_USERNAME: z.string().min(1).optional(),
    KAFKA_SASL_PASSWORD: z.string().min(1).optional(),
    KAFKA_SSL_REJECT_UNAUTHORIZED: boolFromEnv,
    KAFKA_REQUEST_TIMEOUT_MS: z.coerce.number().int().positive().default(30_000),
    KAFKA_CONNECTION_TIMEOUT_MS: z.coerce.number().int().positive().default(10_000),
    KAFKA_RETRY_ATTEMPTS: z.coerce.number().int().min(0).default(5),
    POSTGRES_URL: z
      .string()
      .min(1)
      .default('postgresql://postgres:postgres@localhost:5432/kafka_inspector'),
    REDIS_URL: z.string().min(1).default('redis://localhost:6379'),
    LOG_LEVEL: z.enum(['fatal', 'error', 'warn', 'info', 'debug', 'trace']).default('info'),
    PARTITION_IMBALANCE_RATIO: z.coerce.number().positive().default(5),
    STALL_WINDOW_SECONDS: z.coerce.number().positive().default(300),
  })
  .superRefine((env, ctx) => {
    if (!env.KAFKA_SECURITY_PROTOCOL.startsWith('SASL')) return;
    if (!env.KAFKA_SASL_USERNAME) {
      ctx.addIssue({
        code: 'custom',
        path: ['KAFKA_SASL_USERNAME'],
        message: 'required when KAFKA_SECURITY_PROTOCOL uses SASL',
      });
    }
    if (!env.KAFKA_SASL_PASSWORD) {
      ctx.addIssue({
        code: 'custom',
        path: ['KAFKA_SASL_PASSWORD'],
        message: 'required when KAFKA_SECURITY_PROTOCOL uses SASL',
      });
    }
  });

export type Env = z.infer<typeof envSchema>;

export function loadEnv(source: NodeJS.ProcessEnv = process.env): Env {
  const parsed = envSchema.safeParse(source);
  if (!parsed.success) {
    // Field names, codes and static messages only — never raw values (they may contain credentials).
    const issues = parsed.error.issues.map(
      (issue) => `${issue.path.join('.') || 'env'} (${issue.code}): ${issue.message}`,
    );
    throw new AppError(
      'CONFIG_INVALID',
      `Invalid environment configuration: ${issues.join('; ')}`,
      {
        fields: issues,
      },
    );
  }
  return parsed.data;
}
