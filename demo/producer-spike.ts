import { log, runScenario, sleep } from './kafka.js';

log(
  'Scenario: producer-spike. A burst of records arrives faster than the consumer, then traffic slows.',
);

await runScenario({
  topic: 'spike-events',
  groupId: 'spike-consumer',
  partitions: 3,
  consume: 'steady',
  produce: async (send, running) => {
    let sequence = 0;
    const burst = 2000;
    const warmup = 20;
    log(`Sending ${warmup} warm-up records so the group has committed offsets.`);
    for (let index = 0; index < warmup && running(); index += 1) {
      sequence += 1;
      await send(`key-${sequence % 3}`, JSON.stringify({ sequence, scenario: 'producer-spike' }));
      await sleep(50);
    }
    log('Warm-up complete. Waiting for the consumer to commit before the burst.');
    await sleep(8_000);
    log(`Sending the burst of ${burst} records now.`);
    const burstStart = sequence;
    while (running() && sequence < burstStart + burst) {
      sequence += 1;
      await send(`key-${sequence % 3}`, JSON.stringify({ sequence, scenario: 'producer-spike' }));
    }
    log(`Burst of ${sequence - burstStart} records sent. Switching to a trickle.`);
    while (running()) {
      sequence += 1;
      await send(`key-${sequence % 3}`, JSON.stringify({ sequence, scenario: 'producer-spike' }));
      await sleep(200);
    }
  },
});
