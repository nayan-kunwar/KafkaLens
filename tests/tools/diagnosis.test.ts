import type { CallToolResult } from '@modelcontextprotocol/sdk/types.js';
import { pino } from 'pino';
import { describe, expect, it } from 'vitest';

import { AppError } from '../../src/errors/app-error.js';
import type { DiagnosisService } from '../../src/services/diagnosis-service.js';
import {
  createDiagnoseConsumerGroupHandler,
  createFindSimilarIncidentsHandler,
} from '../../src/tools/diagnosis.js';

const logger = pino({ level: 'silent' });

function fakeService(overrides: Partial<DiagnosisService>): DiagnosisService {
  return overrides as unknown as DiagnosisService;
}

function parseResult(result: CallToolResult): { isError: boolean; data: unknown } {
  const block = result.content[0];
  const text = block !== undefined && block.type === 'text' ? block.text : 'null';
  return { isError: result.isError === true, data: JSON.parse(text) };
}

describe('diagnosis tool handlers', () => {
  it('passes diagnose_consumer_group through', async () => {
    const service = fakeService({
      diagnoseConsumerGroup: (groupId: string, topic: string) =>
        Promise.resolve({ groupId, topic, status: 'no_issue_detected' } as never),
    });
    const { isError, data } = parseResult(
      await createDiagnoseConsumerGroupHandler(
        service,
        logger,
      )({
        groupId: 'payments',
        topic: 'payment-events',
      }),
    );
    expect(isError).toBe(false);
    expect(data).toMatchObject({ groupId: 'payments', topic: 'payment-events' });
  });

  it('returns NOT_FOUND from find_similar_incidents as tool error content', async () => {
    const service = fakeService({
      findSimilarIncidents: () => Promise.reject(new AppError('STORE_UNAVAILABLE', 'down')),
    });
    const { isError, data } = parseResult(
      await createFindSimilarIncidentsHandler(
        service,
        logger,
      )({
        groupId: 'payments',
        topic: 'payment-events',
        limit: 10,
      }),
    );
    expect(isError).toBe(true);
    expect(data).toMatchObject({ code: 'STORE_UNAVAILABLE' });
  });
});
