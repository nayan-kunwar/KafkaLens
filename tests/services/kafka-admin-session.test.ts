import type { Admin, Kafka } from 'kafkajs';
import { describe, expect, it } from 'vitest';

import { AppError } from '../../src/errors/app-error.js';
import { KafkaAdminSession } from '../../src/services/kafka-admin-session.js';

function refused(): Error {
  return Object.assign(new Error('refused'), { code: 'ECONNREFUSED' });
}

describe('KafkaAdminSession', () => {
  it('connects once and runs calls one at a time', async () => {
    let connects = 0;
    let active = 0;
    let maxActive = 0;
    const admin = {
      connect: () => {
        connects += 1;
        return Promise.resolve();
      },
      disconnect: () => Promise.resolve(),
    };
    const kafka = { admin: () => admin } as unknown as Kafka;
    const session = new KafkaAdminSession(kafka);

    const results = await Promise.all([
      session.run(async (client) => {
        active += 1;
        maxActive = Math.max(maxActive, active);
        await new Promise((resolve) => setTimeout(resolve, 20));
        active -= 1;
        return client === (admin as unknown as Admin) ? 1 : 0;
      }),
      session.run(async () => {
        active += 1;
        maxActive = Math.max(maxActive, active);
        await new Promise((resolve) => setTimeout(resolve, 20));
        active -= 1;
        return 2;
      }),
    ]);

    expect(results.sort()).toEqual([1, 2]);
    expect(connects).toBe(1);
    expect(maxActive).toBe(1);
    await session.close();
  });

  it('drops the client after a connection failure and connects again', async () => {
    let connects = 0;
    const kafka = {
      admin: () => ({
        connect: () => {
          connects += 1;
          if (connects === 1) return Promise.reject(refused());
          return Promise.resolve();
        },
        disconnect: () => Promise.resolve(),
      }),
    } as unknown as Kafka;
    const session = new KafkaAdminSession(kafka);

    await expect(session.run(() => Promise.resolve('ok'))).rejects.toBeInstanceOf(AppError);
    await expect(session.run(() => Promise.resolve('ok'))).resolves.toBe('ok');
    expect(connects).toBe(2);
    await session.close();
  });

  it('drops the client when a call fails and disconnects on the way out', async () => {
    let connects = 0;
    let disconnects = 0;
    const kafka = {
      admin: () => ({
        connect: () => {
          connects += 1;
          return Promise.resolve();
        },
        disconnect: () => {
          disconnects += 1;
          return Promise.resolve();
        },
      }),
    } as unknown as Kafka;
    const session = new KafkaAdminSession(kafka);

    await expect(session.run(() => Promise.reject(refused()))).rejects.toBeInstanceOf(AppError);
    await expect(session.run(() => Promise.resolve('ok'))).resolves.toBe('ok');
    expect(connects).toBe(2);
    await session.close();
    expect(disconnects).toBe(2);
  });

  it('rejects runs after close without opening a new connection', async () => {
    let connects = 0;
    const kafka = {
      admin: () => ({
        connect: () => {
          connects += 1;
          return Promise.resolve();
        },
        disconnect: () => Promise.resolve(),
      }),
    } as unknown as Kafka;
    const session = new KafkaAdminSession(kafka);

    await expect(session.run(() => Promise.resolve('ok'))).resolves.toBe('ok');
    await session.close();
    await session.close();
    const thrown = await session.run(() => Promise.resolve('ok')).catch((err: unknown) => err);
    expect(thrown).toBeInstanceOf(AppError);
    expect((thrown as AppError).message).toMatch(/closed/);
    expect(connects).toBe(1);
  });
});
