import type { Admin, Kafka } from 'kafkajs';

import { mapKafkaError } from './kafka-error.js';

export interface BrokerInfo {
  nodeId: number;
  host: string;
  port: number;
}

export interface ClusterInfo {
  clusterId: string;
  controllerId: number | null;
  brokerCount: number;
  brokers: BrokerInfo[];
  observedAt: string;
}

export class AdminService {
  private readonly kafka: Kafka;

  constructor(kafka: Kafka) {
    this.kafka = kafka;
  }

  async getClusterInfo(): Promise<ClusterInfo> {
    return this.withAdmin(async (admin) => {
      const cluster = await admin.describeCluster();
      return {
        clusterId: cluster.clusterId,
        controllerId: cluster.controller,
        brokerCount: cluster.brokers.length,
        brokers: cluster.brokers.map(({ nodeId, host, port }) => ({ nodeId, host, port })),
        observedAt: new Date().toISOString(),
      };
    });
  }

  private async withAdmin<T>(fn: (admin: Admin) => Promise<T>): Promise<T> {
    const admin = this.kafka.admin();
    let connected = false;
    try {
      await admin.connect();
      connected = true;
      return await fn(admin);
    } catch (err) {
      throw mapKafkaError(err);
    } finally {
      if (connected) {
        await admin.disconnect().catch(() => undefined);
      }
    }
  }
}
