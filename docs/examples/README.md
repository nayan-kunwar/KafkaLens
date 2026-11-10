# Captured diagnoses

These are real `diagnose_consumer_group` responses captured on 2026-11-10 from the local
Compose stack. Each scenario used one long-lived MCP session and two calls: the first seeded
history, the second is the payload in the linked JSON file. The JSON is unchanged except for
pretty-printing.

The host already used PostgreSQL port `5432`, so this session published PostgreSQL on `5434`
and Redis on `6380` through a temporary Compose override. Kafka stayed on `9092`. All four
scenarios ran against the same stack and the same MCP server session, so related incidents
can include other demo groups. That is expected and demonstrates deterministic overlap
scoring.

| Scenario          | Topic / group                         | Status    | Findings                                                                                                   | Report                                       |
| ----------------- | ------------------------------------- | --------- | ---------------------------------------------------------------------------------------------------------- | -------------------------------------------- |
| Slow consumer     | `slow-events` / `slow-consumer`       | attention | `CONSUMER_STALLED`, `PRODUCER_OUTPACING_CONSUMER`, `INSUFFICIENT_CONSUMER_CAPACITY`                        | [slow-consumer.md](slow-consumer.md)         |
| Stalled consumer  | `stalled-events` / `stalled-consumer` | attention | `CONSUMER_STALLED`, `PRODUCER_OUTPACING_CONSUMER`, `INSUFFICIENT_CONSUMER_CAPACITY`                        | [stalled-consumer.md](stalled-consumer.md)   |
| Partition hotspot | `hotspot-events` / `hotspot-consumer` | attention | `PARTITION_IMBALANCE`, `CONSUMER_STALLED`, `PRODUCER_OUTPACING_CONSUMER`, `INSUFFICIENT_CONSUMER_CAPACITY` | [partition-hotspot.md](partition-hotspot.md) |
| Producer spike    | `spike-events` / `spike-consumer`     | attention | `PRODUCER_OUTPACING_CONSUMER`, `INSUFFICIENT_CONSUMER_CAPACITY`                                            | [producer-spike.md](producer-spike.md)       |

## Recreating the captures

```bash
npm run demo:up
npm run demo:slow-consumer
```

Each demo script prints the `groupId` and `topic`. Pass them to
`diagnose_consumer_group`. To see progress and throughput findings, call it twice in the same
MCP session with at least the configured stall window between calls.

The spike scenario sends a short warm-up, waits for the consumer to commit, then sends the
burst. That makes the rate gap visible in the second sample instead of disappearing before
the first one.
