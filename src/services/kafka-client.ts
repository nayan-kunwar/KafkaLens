import kafkajs from 'kafkajs';
import type { Kafka as KafkaType, KafkaConfig, logCreator, SASLOptions } from 'kafkajs';
import { type Logger } from 'pino';

import type { Env } from '../config/env.js';

const { Kafka, logLevel } = kafkajs;

export function parseBrokers(raw: string): string[] {
  const brokers = raw
    .split(',')
    .map((entry) => entry.trim())
    .filter((entry) => entry.length > 0);
  return brokers.length > 0 ? brokers : ['localhost:9092'];
}

function buildSasl(env: Env): SASLOptions {
  const username = env.KAFKA_SASL_USERNAME ?? '';
  const password = env.KAFKA_SASL_PASSWORD ?? '';
  switch (env.KAFKA_SASL_MECHANISM) {
    case 'plain':
      return { mechanism: 'plain', username, password };
    case 'scram-sha-256':
      return { mechanism: 'scram-sha-256', username, password };
    case 'scram-sha-512':
      return { mechanism: 'scram-sha-512', username, password };
  }
}

export function buildKafkaConfig(env: Env): KafkaConfig {
  const config: KafkaConfig = {
    brokers: parseBrokers(env.KAFKA_BROKERS),
    clientId: env.KAFKA_CLIENT_ID,
    connectionTimeout: env.KAFKA_CONNECTION_TIMEOUT_MS,
    requestTimeout: env.KAFKA_REQUEST_TIMEOUT_MS,
    retry: { retries: env.KAFKA_RETRY_ATTEMPTS },
  };

  if (env.KAFKA_SECURITY_PROTOCOL === 'SSL' || env.KAFKA_SECURITY_PROTOCOL === 'SASL_SSL') {
    config.ssl = { rejectUnauthorized: env.KAFKA_SSL_REJECT_UNAUTHORIZED };
  }
  if (env.KAFKA_SECURITY_PROTOCOL.startsWith('SASL')) {
    config.sasl = buildSasl(env);
  }
  return config;
}

function pinoLogCreator(logger: Logger): logCreator {
  return () =>
    ({ namespace, level, log }) => {
      const child = logger.child({ kafkaNamespace: namespace });
      const { message, timestamp, ...rest } = log;
      const fields = { ...rest, timestamp };
      switch (level) {
        case logLevel.ERROR:
          child.error(fields, message);
          break;
        case logLevel.WARN:
          child.warn(fields, message);
          break;
        case logLevel.INFO:
          child.info(fields, message);
          break;
        case logLevel.DEBUG:
          child.debug(fields, message);
          break;
        default:
          break;
      }
    };
}

export function createKafka(env: Env, logger?: Logger): KafkaType {
  const config = buildKafkaConfig(env);
  if (logger) {
    config.logCreator = pinoLogCreator(logger);
  }
  return new Kafka(config);
}
