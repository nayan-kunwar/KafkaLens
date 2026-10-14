import { log, runScenario, sleep } from './kafka.js';

log(
  'Scenario: partition-hotspot. About 95% of records use one key so one partition collects the lag.',
);

await runScenario({
  topic: 'hotspot-events',
  groupId: 'hotspot-consumer',
  partitions: 6,
  consume: 'steady',
  produce: async (send, running) => {
    let sequence = 0;
    while (running()) {
      sequence += 1;
      const hot = sequence % 20 !== 0;
      const key = hot ? 'hot-key' : `other-${sequence}`;
      await send(key, JSON.stringify({ sequence, scenario: 'partition-hotspot' }));
      await sleep(20);
    }
  },
});
