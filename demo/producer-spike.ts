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
    while (running() && sequence < burst) {
      sequence += 1;
      await send(`key-${sequence % 3}`, JSON.stringify({ sequence, scenario: 'producer-spike' }));
    }
    log(`Burst of ${sequence} records sent. Switching to a trickle.`);
    while (running()) {
      sequence += 1;
      await send(`key-${sequence % 3}`, JSON.stringify({ sequence, scenario: 'producer-spike' }));
      await sleep(200);
    }
  },
});
