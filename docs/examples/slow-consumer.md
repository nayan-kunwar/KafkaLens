# Slow consumer investigation

Captured on 2026-11-10 from the local Compose stack. The payload in
[slow-consumer-diagnosis.json](slow-consumer-diagnosis.json) is the second
`diagnose_consumer_group` response, unchanged except for pretty-printing.

## How it was produced

`demo/slow-consumer.ts` created topic `slow-events` with 3 partitions and group
`slow-consumer`. The handler waits 1.5 seconds per message while the producer writes every
100ms. The scenario was configured for 140 seconds.

The inspector sampled the group twice over one long-lived MCP session: at
`2026-11-10T13:44:13.445Z` and `2026-11-10T13:45:08.586Z` (55.1 seconds apart). The first
call seeded history; the second call produced the payload below. The scenario started at
`2026-11-10T13:43:36.773Z`.

| Setting                     | Value            |
| --------------------------- | ---------------- |
| `KAFKA_BROKERS`             | `localhost:9092` |
| `STALL_WINDOW_SECONDS`      | `60`             |
| `PARTITION_IMBALANCE_RATIO` | `5`              |

## What the tool returned

`status` is `attention`. Top confidence is `0.84`.

| Finding                          | Severity | Confidence |
| -------------------------------- | -------- | ---------- |
| `CONSUMER_STALLED`               | high     | 0.84       |
| `PRODUCER_OUTPACING_CONSUMER`    | high     | 0.78       |
| `INSUFFICIENT_CONSUMER_CAPACITY` | medium   | 0.58       |

Known lag was `827` across all 3 partitions (`276`, `275`, `276`). The group was `Stable`
with 1 member, and every partition was assigned. Lag was evenly spread, with a max/min ratio
of `1` against the configured `5`.

Rates are labeled estimates over about 55.1 seconds: incoming `9.176` messages/second,
processing `0`, lag growth `9.176`. The tool calls the gap a possible reason for the lag
increase and states that the rates are not broker producer metrics.

The committed offset stayed at `5` on all three partitions during the window. The handler
was still being invoked slowly (1.5 seconds per message), so this is evidence of no
committed progress in the sample, not proof that the process crashed. The tool's wording is
explicit: "This shows no progress. It does not show that the process crashed."

`partialFailures` is empty. Change correlation is `not_configured`. Related incidents are
deterministic overlaps with other demo groups from the same session.

## Sample finding

```json
{
  "type": "CONSUMER_STALLED",
  "severity": "high",
  "confidence": 0.84,
  "evidence": [
    "Partition 0 is assigned and its committed offset stayed at 5 between 2026-11-10T13:44:13.445Z to 2026-11-10T13:45:08.586Z while lag was 276. This shows no progress. It does not show that the process crashed.",
    "Partition 1 is assigned and its committed offset stayed at 5 between 2026-11-10T13:44:13.445Z to 2026-11-10T13:45:08.586Z while lag was 275. This shows no progress. It does not show that the process crashed.",
    "Partition 2 is assigned and its committed offset stayed at 5 between 2026-11-10T13:44:13.445Z to 2026-11-10T13:45:08.586Z while lag was 276. This shows no progress. It does not show that the process crashed."
  ],
  "recommendations": [
    "Check the member assigned to the stalled partition for a blocked handler or a paused consumer over the 60s window.",
    "Do not treat a lack of offset progress as proof that the process has crashed.",
    "Re-run the diagnosis after the stall window if this sample does not cover it."
  ]
}
```
