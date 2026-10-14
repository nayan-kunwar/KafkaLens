import { describe, expect, it } from 'vitest';

import { AppError } from '../../src/errors/app-error.js';
import {
  assertReadOnlyTool,
  assertSafetyBoundary,
  FORBIDDEN_KAFKA_OPERATIONS,
  READ_ONLY_TOOL_NAMES,
  rejectKafkaMutation,
} from '../../src/safety/readonly.js';

describe('read-only safety boundary', () => {
  it('accepts every registered investigation tool', () => {
    expect(() => assertSafetyBoundary()).not.toThrow();
    for (const name of READ_ONLY_TOOL_NAMES) {
      expect(() => assertReadOnlyTool(name)).not.toThrow();
    }
  });

  it('rejects destructive Kafka operations', () => {
    for (const name of FORBIDDEN_KAFKA_OPERATIONS) {
      expect(() => assertReadOnlyTool(name)).toThrow(AppError);
      try {
        rejectKafkaMutation(name);
      } catch (err) {
        expect(err).toBeInstanceOf(AppError);
        const error = err as AppError;
        expect(error.code).toBe('SAFETY_REJECTED');
        expect(error.message).toContain(name);
        expect(error.message).not.toContain('postgres://');
      }
    }
  });

  it('rejects an unknown tool name', () => {
    expect(() => assertReadOnlyTool('drop_cluster')).toThrow(AppError);
  });
});
