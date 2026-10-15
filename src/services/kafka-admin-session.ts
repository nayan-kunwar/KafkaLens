import type { Admin, Kafka } from 'kafkajs';

import { AppError } from '../errors/app-error.js';

import { mapKafkaError } from './kafka-error.js';

/**
 * One Kafka admin connection for the process. Calls are serialized because a
 * single admin client must not run two requests at once. A connection failure
 * drops the client so the next call connects again.
 *
 * Prefer sharing one session per process and closing it on shutdown. The
 * `?? new KafkaAdminSession(kafka)` fallbacks in the services exist for
 * tests and are caller-owned: whoever relies on the fallback must close it.
 */
export class KafkaAdminSession {
  private admin: Admin | undefined;
  private connecting: Promise<Admin> | undefined;
  private tail: Promise<void> = Promise.resolve();
  private closed = false;

  constructor(private readonly kafka: Kafka) {}

  run<T>(fn: (admin: Admin) => Promise<T>): Promise<T> {
    if (this.closed) {
      return Promise.reject(new AppError('INTERNAL', 'Kafka admin session is closed'));
    }
    const task = this.tail.then(() => this.invoke(fn));
    this.tail = task.then(
      () => undefined,
      () => undefined,
    );
    return task;
  }

  async close(): Promise<void> {
    if (this.closed) return;
    this.closed = true;
    await this.tail.catch(() => undefined);
    await this.drop();
  }

  private async invoke<T>(fn: (admin: Admin) => Promise<T>): Promise<T> {
    try {
      const admin = await this.ensure();
      return await fn(admin);
    } catch (err) {
      const mapped = mapKafkaError(err);
      if (mapped.code === 'KAFKA_UNAVAILABLE') {
        await this.drop();
      }
      throw mapped;
    }
  }

  private async ensure(): Promise<Admin> {
    if (this.admin) return this.admin;
    this.connecting ??= this.open().finally(() => {
      this.connecting = undefined;
    });
    return this.connecting;
  }

  private async open(): Promise<Admin> {
    const admin = this.kafka.admin();
    try {
      await admin.connect();
    } catch (err) {
      await admin.disconnect().catch(() => undefined);
      throw err;
    }
    this.admin = admin;
    return admin;
  }

  private async drop(): Promise<void> {
    const admin = this.admin;
    this.admin = undefined;
    if (admin) await admin.disconnect().catch(() => undefined);
  }
}
