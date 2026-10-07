import kafkajs from 'kafkajs';

import { AppError, type ErrorCode } from '../errors/app-error.js';

const {
  KafkaJSConnectionError,
  KafkaJSError,
  KafkaJSNumberOfRetriesExceeded,
  KafkaJSProtocolError,
  KafkaJSRequestTimeoutError,
  KafkaJSSASLAuthenticationError,
} = kafkajs;

const AUTH_PROTOCOL_ERROR_TYPES = new Set([
  'ILLEGAL_SASL_STATE',
  'UNSUPPORTED_SASL_MECHANISM',
  'SASL_AUTHENTICATION_FAILED',
]);

const NOT_FOUND_PROTOCOL_ERROR_TYPES = new Set([
  'UNKNOWN_TOPIC_OR_PARTITION',
  'UNKNOWN_TOPIC_ID',
  'LEADER_NOT_AVAILABLE',
]);

const UNAVAILABLE_NAME_ERRORS = new Set([
  'KafkaJSNoBrokerAvailableError',
  'KafkaJSBrokerNotFound',
  'KafkaJSMetadataNotLoaded',
]);

const UNAVAILABLE_NODE_ERROR_CODES = new Set([
  'ECONNREFUSED',
  'ECONNRESET',
  'ETIMEDOUT',
  'EHOSTUNREACH',
  'ENETUNREACH',
  'ENOTFOUND',
  'EAI_AGAIN',
]);

function redactUserInfo(value: string): string {
  return value.replace(/\/\/[^/\s@]+@/g, '//').replace(/^[^/@\s]+:[^/@\s]+@/, '');
}

function nodeErrorCode(err: unknown): string | undefined {
  if (typeof err !== 'object' || err === null || !('code' in err)) return undefined;
  const { code } = err;
  return typeof code === 'string' ? code : undefined;
}

function errorContext(err: unknown): Record<string, unknown> | undefined {
  const context: Record<string, unknown> = {};
  if (err instanceof KafkaJSError) {
    context.retryable = err.retriable;
  }
  if (err instanceof KafkaJSProtocolError && err.type) {
    context.kafkaType = err.type;
  }
  if (err instanceof KafkaJSConnectionError && err.broker) {
    context.broker = redactUserInfo(err.broker);
  }
  if (err instanceof KafkaJSRequestTimeoutError && err.broker) {
    context.broker = redactUserInfo(err.broker);
  }
  return Object.keys(context).length > 0 ? context : undefined;
}

function appErrorFor(code: ErrorCode, message: string, err: unknown): AppError {
  const context = errorContext(err);
  return context === undefined ? new AppError(code, message) : new AppError(code, message, context);
}

function mapError(err: unknown): AppError {
  if (err instanceof KafkaJSSASLAuthenticationError) {
    return appErrorFor('AUTH_FAILED', 'Kafka SASL authentication failed', err);
  }

  if (err instanceof KafkaJSProtocolError) {
    if (err.type && AUTH_PROTOCOL_ERROR_TYPES.has(err.type)) {
      return appErrorFor('AUTH_FAILED', `Kafka protocol error: ${err.type}`, err);
    }
    if (err.type && NOT_FOUND_PROTOCOL_ERROR_TYPES.has(err.type)) {
      return appErrorFor('NOT_FOUND', `Kafka protocol error: ${err.type}`, err);
    }
    return appErrorFor('KAFKA_ERROR', `Kafka protocol error: ${err.type ?? 'UNKNOWN'}`, err);
  }

  if (err instanceof KafkaJSConnectionError || err instanceof KafkaJSRequestTimeoutError) {
    return appErrorFor('KAFKA_UNAVAILABLE', 'Kafka broker unreachable', err);
  }

  if (err instanceof Error && UNAVAILABLE_NAME_ERRORS.has(err.name)) {
    return appErrorFor('KAFKA_UNAVAILABLE', 'No Kafka broker available', err);
  }

  const nodeCode = nodeErrorCode(err);
  if (nodeCode && UNAVAILABLE_NODE_ERROR_CODES.has(nodeCode)) {
    return appErrorFor('KAFKA_UNAVAILABLE', `Kafka connection failed (${nodeCode})`, err);
  }

  if (err instanceof KafkaJSNumberOfRetriesExceeded && err.cause) {
    return mapError(err.cause);
  }

  if (err instanceof KafkaJSError) {
    return appErrorFor('KAFKA_UNAVAILABLE', 'Kafka request failed', err);
  }

  return appErrorFor('KAFKA_ERROR', 'Unexpected Kafka client error', err);
}

export function mapKafkaError(err: unknown): AppError {
  if (err instanceof AppError) return err;
  if (!(err instanceof Error)) {
    return new AppError('KAFKA_ERROR', 'Unexpected Kafka client error');
  }
  try {
    return mapError(err);
  } catch {
    return new AppError('KAFKA_ERROR', 'Unexpected Kafka client error');
  }
}
