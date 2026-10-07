import kafkajs from 'kafkajs';
import type {
  KafkaJSSASLAuthenticationError as SaslAuthErrorType,
  KafkaJSProtocolError as ProtocolErrorType,
} from 'kafkajs';
import { describe, expect, it } from 'vitest';

import { AppError } from '../../src/errors/app-error.js';
import { mapKafkaError } from '../../src/services/kafka-error.js';

const {
  KafkaJSConnectionError,
  KafkaJSError,
  KafkaJSNumberOfRetriesExceeded,
  KafkaJSProtocolError,
  KafkaJSRequestTimeoutError,
  KafkaJSSASLAuthenticationError,
} = kafkajs;

function protocolError(type: string): ProtocolErrorType {
  const err = Object.assign(new Error(`protocol error ${type}`), { type, code: 3 });
  return new KafkaJSProtocolError(err);
}

describe('mapKafkaError', () => {
  it('passes AppError through unchanged', () => {
    const original = new AppError('NOT_FOUND', 'already mapped');
    expect(mapKafkaError(original)).toBe(original);
  });

  it('maps connection errors to KAFKA_UNAVAILABLE and redacts broker userinfo', () => {
    const err = new KafkaJSConnectionError('connection refused', {
      broker: 'alice:s3cr3t@broker:9092',
      code: 'ECONNREFUSED',
    });
    const mapped = mapKafkaError(err);
    expect(mapped.code).toBe('KAFKA_UNAVAILABLE');
    expect(mapped.context?.broker).toBe('broker:9092');
    expect(mapped.context?.retryable).toBe(true);
    expect(mapped.message).not.toContain('s3cr3t');
  });

  it('maps request timeouts to KAFKA_UNAVAILABLE', () => {
    const err = new KafkaJSRequestTimeoutError('request timed out', {
      broker: 'broker:9092',
      clientId: 'kafka-inspector',
      correlationId: 1,
      createdAt: 1,
      sentAt: 1,
      pendingDuration: 100,
    });
    expect(mapKafkaError(err).code).toBe('KAFKA_UNAVAILABLE');
  });

  it('maps SASL authentication failures to AUTH_FAILED', () => {
    const SaslAuthError = KafkaJSSASLAuthenticationError as unknown as new (
      message: string,
    ) => SaslAuthErrorType;
    const mapped = mapKafkaError(new SaslAuthError('SASL PLAIN: Invalid username or password'));
    expect(mapped.code).toBe('AUTH_FAILED');
  });

  it('maps ILLEGAL_SASL_STATE protocol errors to AUTH_FAILED', () => {
    const mapped = mapKafkaError(protocolError('ILLEGAL_SASL_STATE'));
    expect(mapped.code).toBe('AUTH_FAILED');
    expect(mapped.context?.kafkaType).toBe('ILLEGAL_SASL_STATE');
  });

  it('maps unknown-topic protocol errors to NOT_FOUND', () => {
    const mapped = mapKafkaError(protocolError('UNKNOWN_TOPIC_OR_PARTITION'));
    expect(mapped.code).toBe('NOT_FOUND');
    expect(mapped.context?.kafkaType).toBe('UNKNOWN_TOPIC_OR_PARTITION');
  });

  it('maps other protocol errors to KAFKA_ERROR', () => {
    expect(mapKafkaError(protocolError('UNKNOWN_SERVER_ERROR')).code).toBe('KAFKA_ERROR');
  });

  it('maps Node network failures to KAFKA_UNAVAILABLE', () => {
    const err = Object.assign(new Error('connect ECONNREFUSED 127.0.0.1:9092'), {
      code: 'ECONNREFUSED',
    });
    expect(mapKafkaError(err).code).toBe('KAFKA_UNAVAILABLE');
  });

  it('maps no-broker-available by error name to KAFKA_UNAVAILABLE', () => {
    const err = Object.assign(new Error('No broker available'), {
      name: 'KafkaJSNoBrokerAvailableError',
    });
    expect(mapKafkaError(err).code).toBe('KAFKA_UNAVAILABLE');
  });

  it('maps KafkaJSNumberOfRetriesExceeded via its cause', () => {
    const cause = new KafkaJSConnectionError('connection refused', { broker: 'broker:9092' });
    const err = new KafkaJSNumberOfRetriesExceeded(cause, { retryCount: 5, retryTime: 1000 });
    expect(mapKafkaError(err).code).toBe('KAFKA_UNAVAILABLE');
  });

  it('maps generic retriable KafkaJS errors to KAFKA_UNAVAILABLE', () => {
    expect(mapKafkaError(new KafkaJSError('something transient')).code).toBe('KAFKA_UNAVAILABLE');
  });

  it('maps non-Error throwables to KAFKA_ERROR', () => {
    expect(mapKafkaError('boom').code).toBe('KAFKA_ERROR');
  });

  it('maps unexpected errors to KAFKA_ERROR', () => {
    expect(mapKafkaError(new Error('boom')).code).toBe('KAFKA_ERROR');
  });
});
