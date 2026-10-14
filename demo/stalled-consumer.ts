import { log, runScenario, sleep } from './kafka.js';

log(
  'Scenario: stalled-consumer. The consumer joins, reads a few records, then pauses while producing continues.',
);

await runScenario({
  topic: 'stalled-events',
  groupId: 'stalled-consumer',
  partitions: 3,
  consume: 'stalled',
  produce: async (send, running) => {
    let sequence = 0;
    while (running()) {
      sequence += 1;
      await send(`key-${sequence % 3}`, JSON.stringify({ sequence, scenario: 'stalled-consumer' }));
      await sleep(50);
    }
  },
});
