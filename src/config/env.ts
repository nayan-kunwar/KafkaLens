import { z } from 'zod';

import { AppError } from '../errors/app-error.js';

function boolEnv(fallback: 'true' | 'false') {
  return z
    .string()
    .default(fallback)
    .transform((value, ctx) => {
      if (value === 'true') return true;
      if (value === 'false') return false;
      ctx.addIssue({ code: 'custom', message: 'must be "true" or "false"' });
      return z.NEVER;
    });
}

const DEFAULT_KAFKA_BROKERS = 'localhost:9092';
const DEFAULT_POSTGRES_URL = 'postgresql://postgres:postgres@localhost:5432/kafka_inspector';

/**
 * Bounded integer that rejects empty strings. Plain `z.coerce.number` turns
 * `''` into `0`, which would silently disable retention instead of failing
 * fast, so blank input is mapped to NaN (always invalid) first. The upper
 * bound also rejects non-finite values such as `Infinity`.
 */
function boundedIntEnv(fallback: number, min: number, max: number) {
  return z.preprocess(
    (value) => (typeof value === 'string' && value.trim() === '' ? Number.NaN : value),
    z.coerce.number().int().min(min).max(max).default(fallback),
  );
}

function boundedNumberEnv(fallback: number, min: number, max: number) {
  return z.preprocess(
    (value) => (typeof value === 'string' && value.trim() === '' ? Number.NaN : value),
    z.coerce.number().min(min).max(max).default(fallback),
  );
}

const envSchema = z
  .object({
    NODE_ENV: z.enum(['development', 'test', 'production']).default('development'),
    KAFKA_BROKERS: z.string().min(1).default(DEFAULT_KAFKA_BROKERS),
    KAFKA_CLIENT_ID: z.string().min(1).default('kafka-inspector'),
    KAFKA_SECURITY_PROTOCOL: z
      .enum(['PLAINTEXT', 'SSL', 'SASL_PLAINTEXT', 'SASL_SSL'])
      .default('PLAINTEXT'),
    KAFKA_SASL_MECHANISM: z.enum(['plain', 'scram-sha-256', 'scram-sha-512']).default('plain'),
    KAFKA_SASL_USERNAME: z.string().min(1).optional(),
    KAFKA_SASL_PASSWORD: z.string().min(1).optional(),
    KAFKA_SSL_REJECT_UNAUTHORIZED: boolEnv('true'),
    KAFKA_REQUEST_TIMEOUT_MS: boundedIntEnv(30_000, 100, 300_000),
    KAFKA_CONNECTION_TIMEOUT_MS: boundedIntEnv(10_000, 100, 300_000),
    KAFKA_RETRY_ATTEMPTS: boundedIntEnv(5, 0, 100),
    POSTGRES_URL: z.string().min(1).default(DEFAULT_POSTGRES_URL),
    POSTGRES_CONNECTION_TIMEOUT_MS: boundedIntEnv(5_000, 100, 300_000),
    POSTGRES_STATEMENT_TIMEOUT_MS: boundedIntEnv(10_000, 100, 300_000),
    POSTGRES_QUERY_TIMEOUT_MS: boundedIntEnv(15_000, 100, 300_000),
    REDIS_URL: z.string().min(1).default('redis://localhost:6379'),
    LOG_LEVEL: z.enum(['fatal', 'error', 'warn', 'info', 'debug', 'trace']).default('info'),
    PARTITION_IMBALANCE_RATIO: boundedNumberEnv(5, 1, 1_000_000),
    STALL_WINDOW_SECONDS: boundedNumberEnv(300, 1, 604_800),
    CACHE_TTL_SECONDS: boundedIntEnv(15, 1, 86_400),
    TOOL_TIMEOUT_MS: boundedIntEnv(60_000, 100, 600_000),
    METRICS_ENABLED: boolEnv('false'),
    METRICS_HOST: z.string().min(1).max(255).default('127.0.0.1'),
    METRICS_PORT: z.coerce.number().int().min(0).max(65535).default(9464),
    GITHUB_REPOSITORY: z
      .string()
      .regex(/^[A-Za-z0-9_.-]+\/[A-Za-z0-9_.-]+$/)
      .optional(),
    GITHUB_TOKEN: z.string().min(1).optional(),
    DEPLOYMENT_SOURCE_URL: z.string().url().optional(),
    LAG_RETENTION_DAYS: boundedIntEnv(14, 0, 36_500),
    INCIDENT_RETENTION_DAYS: boundedIntEnv(90, 0, 36_500),
  })
  .superRefine((env, ctx) => {
    if (env.NODE_ENV === 'production') {
      if (env.KAFKA_BROKERS === DEFAULT_KAFKA_BROKERS) {
        ctx.addIssue({
          code: 'custom',
          path: ['KAFKA_BROKERS'],
          message: 'must be set explicitly in production; the localhost default is not allowed',
        });
      }
      if (env.POSTGRES_URL === DEFAULT_POSTGRES_URL) {
        ctx.addIssue({
          code: 'custom',
          path: ['POSTGRES_URL'],
          message: 'must be set explicitly in production; the localhost default is not allowed',
        });
      }
    }
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
