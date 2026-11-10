# Stalled consumer investigation

Captured on 2026-11-10 from the local Compose stack. The payload in
[stalled-consumer-diagnosis.json](stalled-consumer-diagnosis.json) is the second
`diagnose_consumer_group` response, unchanged except for pretty-printing.

## How it was produced

`demo/stalled-consumer.ts` created topic `stalled-events` with 3 partitions and group
`stalled-consumer`. The consumer joins, handles five messages, then pauses fetching while
the producer keeps writing every 50ms. The scenario was configured for 140 seconds.

The inspector sampled the group twice over one long-lived MCP session: at
`2026-11-10T13:44:13.507Z` and `2026-11-10T13:45:08.644Z` (55.1 seconds apart). The first
call seeded history; the second call produced the payload below. The consumer paused at
`2026-11-10T13:43:37.360Z`, about one second after joining.

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

Known lag was `1467`, exactly `489` on each of the 3 partitions. The group was `Stable` with
1 member, and every partition was assigned. Lag was balanced, with a max/min ratio of `1`
against the configured `5`.

Rates are labeled estimates over about 55.1 seconds: incoming `16.033` messages/second,
processing `0`, lag growth `16.033`. Committed offsets stayed at `2`, `1`, and `2` during the
window. That is consistent with the scenario's deliberate pause, but the tool still states
that no offset progress is not proof of a crash.

`partialFailures` is empty. Change correlation is `not_configured`. Related incidents include
the slow-consumer sample and previous demo runs from the same session; scores are
deterministic overlaps.

## Sample finding

```json
{
  "type": "CONSUMER_STALLED",
  "severity": "high",
  "confidence": 0.84,
  "evidence": [
    "Partition 0 is assigned and its committed offset stayed at 2 between 2026-11-10T13:44:13.507Z to 2026-11-10T13:45:08.644Z while lag was 489. This shows no progress. It does not show that the process crashed.",
    "Partition 1 is assigned and its committed offset stayed at 1 between 2026-11-10T13:44:13.507Z to 2026-11-10T13:45:08.644Z while lag was 489. This shows no progress. It does not show that the process crashed.",
    "Partition 2 is assigned and its committed offset stayed at 2 between 2026-11-10T13:44:13.507Z to 2026-11-10T13:45:08.644Z while lag was 489. This shows no progress. It does not show that the process crashed."
  ],
  "recommendations": [
    "Check the member assigned to the stalled partition for a blocked handler or a paused consumer over the 60s window.",
    "Do not treat a lack of offset progress as proof that the process has crashed.",
    "Re-run the diagnosis after the stall window if this sample does not cover it."
  ]
}
```
