import kafkajs from 'kafkajs';

const { Kafka, Partitioners } = kafkajs;

export type DemoKafka = InstanceType<typeof Kafka>;

export function createDemoKafka(): DemoKafka {
  const brokers = (process.env.KAFKA_BROKERS ?? 'localhost:9092')
    .split(',')
    .map((entry) => entry.trim())
    .filter((entry) => entry.length > 0);
  return new Kafka({
    clientId: 'kafka-lens-demo',
    brokers: brokers.length > 0 ? brokers : ['localhost:9092'],
  });
}

export function durationMs(): number {
  const seconds = Number(process.env.DEMO_DURATION_SECONDS ?? '60');
  if (!Number.isFinite(seconds) || seconds <= 0) return 60_000;
  return seconds * 1000;
}

export function log(message: string): void {
  process.stderr.write(`${message}\n`);
}

export function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

export async function ensureTopic(
  kafka: DemoKafka,
  topic: string,
  partitions: number,
): Promise<void> {
  const admin = kafka.admin();
  await admin.connect();
  try {
    await admin.createTopics({
      waitForLeaders: true,
      topics: [{ topic, numPartitions: partitions, replicationFactor: 1 }],
    });
  } finally {
    await admin.disconnect();
  }
}

export async function runScenario(options: {
  topic: string;
  groupId: string;
  partitions: number;
  produce: (
    send: (key: string, value: string) => Promise<void>,
    running: () => boolean,
  ) => Promise<void>;
  consume: 'slow' | 'stalled' | 'steady';
}): Promise<void> {
  const kafka = createDemoKafka();
  await ensureTopic(kafka, options.topic, options.partitions);
  const deadline = Date.now() + durationMs();
  const running = (): boolean => Date.now() < deadline;
  const producer = kafka.producer({ createPartitioner: Partitioners.DefaultPartitioner });
  const consumer = kafka.consumer({ groupId: options.groupId });
  await producer.connect();
  await consumer.connect();
  await consumer.subscribe({ topic: options.topic, fromBeginning: true });

  log(
    `Demo ${options.groupId} on ${options.topic} for ${Math.round(durationMs() / 1000)}s. ` +
      `Investigate with diagnose_consumer_group groupId=${options.groupId} topic=${options.topic}.`,
  );

  let seen = 0;
  const consumption = consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      if (!running()) return;
      seen += 1;
      if (options.consume === 'stalled' && seen >= 5) {
        consumer.pause([{ topic }]);
        log(
          `Paused ${options.groupId} on ${topic} after ${seen} messages (partition ${partition}).`,
        );
        return;
      }
      const delay = options.consume === 'slow' ? 1500 : 80;
      await sleep(delay);
      if (message.offset !== undefined && seen % 20 === 0) {
        log(
          `${options.groupId} committed through offset ${message.offset} on partition ${partition}.`,
        );
      }
    },
  });

  const production = options.produce(async (key, value) => {
    if (!running()) return;
    await producer.send({
      topic: options.topic,
      messages: [{ key, value }],
    });
  }, running);

  try {
    await Promise.race([Promise.all([production, consumption]), sleep(durationMs() + 1000)]);
  } finally {
    await consumer.stop().catch(() => undefined);
    await consumer.disconnect().catch(() => undefined);
    await producer.disconnect().catch(() => undefined);
    log(`Demo ${options.groupId} finished. Messages observed by the consumer loop: ${seen}.`);
  }
}
