# Producer spike investigation

Captured on 2026-11-10 from the local Compose stack. The payload in
[producer-spike-diagnosis.json](producer-spike-diagnosis.json) is the second
`diagnose_consumer_group` response, unchanged except for pretty-printing.

## How it was produced

`demo/producer-spike.ts` created topic `spike-events` with 3 partitions and group
`spike-consumer`. The producer sends 20 warm-up records, waits for the consumer to commit,
sends a burst of 2,000 records, then drops to a trickle every 200ms. The handler waits 80ms
per message. The scenario was configured for 140 seconds.

The inspector sampled the group twice over one long-lived MCP session: at
`2026-11-10T13:50:52.781Z` (before the burst) and `2026-11-10T13:51:19.941Z` (27.1 seconds
later, after the burst). The first call replaced the insufficient-history verdict with a
baseline; the second call produced the payload below. The scenario joined the group at
`2026-11-10T13:50:43.494Z`.

| Setting                     | Value            |
| --------------------------- | ---------------- |
| `KAFKA_BROKERS`             | `localhost:9092` |
| `STALL_WINDOW_SECONDS`      | `60`             |
| `PARTITION_IMBALANCE_RATIO` | `5`              |

## What the tool returned

`status` is `attention`. Top confidence is `0.75`.

| Finding                          | Severity | Confidence |
| -------------------------------- | -------- | ---------- |
| `PRODUCER_OUTPACING_CONSUMER`    | high     | 0.75       |
| `INSUFFICIENT_CONSUMER_CAPACITY` | medium   | 0.58       |

Known lag was `2065` across all 3 partitions (`688`, `688`, `689`). The group was `Stable`
with 1 member, and every partition was assigned. Lag was balanced, with a max/min ratio of
`1` against the configured `5`.

Rates are labeled estimates over about 27.1 seconds: incoming `76.449` messages/second,
processing `2.031`, lag growth `74.417`. This window intentionally contains the burst. The
tool describes the rate gap as a possible reason for the increase and states that the rates
are not broker producer metrics.

The stall check has insufficient evidence because the sample spans 27 seconds, below 80% of
the 60-second window. That is expected for a short spike capture and is different from
claiming the consumer is healthy.

`partialFailures` is empty. Change correlation is `not_configured`. Related incidents are
deterministic overlaps with other demo groups and an earlier spike run from the same session.

## Sample finding

```json
{
  "type": "PRODUCER_OUTPACING_CONSUMER",
  "severity": "high",
  "confidence": 0.75,
  "evidence": [
    "Estimated incoming rate: 76.449 messages/second over about 27.077s (0 partitions excluded).",
    "Estimated processing rate: 2.031 messages/second over about 27.077s (0 partitions excluded).",
    "Estimated lag growth: 74.417 messages/second over about 27.077s (0 partitions excluded).",
    "These rates are estimates from stored high watermarks and committed offsets. They are not broker producer metrics.",
    "Estimated incoming rate exceeded estimated processing rate while lag was growing. That is a possible reason for the increase."
  ],
  "recommendations": [
    "Treat the rates as estimates and confirm them with producer metrics if you have them.",
    "Inspect consumer handler time before changing partition count or producer traffic."
  ]
}
```
