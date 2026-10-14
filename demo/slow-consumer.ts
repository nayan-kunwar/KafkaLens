import { log, runScenario, sleep } from './kafka.js';

log(
  'Scenario: slow-consumer. The consumer waits 1.5s per message while the producer keeps writing.',
);

await runScenario({
  topic: 'slow-events',
  groupId: 'slow-consumer',
  partitions: 3,
  consume: 'slow',
  produce: async (send, running) => {
    let sequence = 0;
    while (running()) {
      sequence += 1;
      await send(`key-${sequence % 3}`, JSON.stringify({ sequence, scenario: 'slow-consumer' }));
      await sleep(100);
    }
  },
});
