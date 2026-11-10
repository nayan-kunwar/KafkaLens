# Partition hotspot investigation

Captured on 2026-11-10 from the local Compose stack. The payload in
[partition-hotspot-diagnosis.json](partition-hotspot-diagnosis.json) is the second
`diagnose_consumer_group` response, unchanged except for pretty-printing.

## How it was produced

`demo/partition-hotspot.ts` created topic `hotspot-events` with 6 partitions and group
`hotspot-consumer`. About 95% of records use the key `hot-key`. The handler waits 80ms and
the producer writes every 20ms. The scenario was configured for 140 seconds.

The inspector sampled the group twice over one long-lived MCP session: at
`2026-11-10T13:44:13.537Z` and `2026-11-10T13:45:08.681Z` (55.1 seconds apart). The first
call seeded history; the second call produced the payload below. The scenario started at
`2026-11-10T13:43:36.865Z`.

| Setting                     | Value            |
| --------------------------- | ---------------- |
| `KAFKA_BROKERS`             | `localhost:9092` |
| `STALL_WINDOW_SECONDS`      | `60`             |
| `PARTITION_IMBALANCE_RATIO` | `5`              |

## What the tool returned

`status` is `attention`. Top confidence is `0.95`.

| Finding                          | Severity | Confidence |
| -------------------------------- | -------- | ---------- |
| `PARTITION_IMBALANCE`            | high     | 0.95       |
| `CONSUMER_STALLED`               | high     | 0.84       |
| `PRODUCER_OUTPACING_CONSUMER`    | high     | 0.78       |
| `INSUFFICIENT_CONSUMER_CAPACITY` | medium   | 0.58       |

Known lag was `2573` across all 6 partitions. Partition 3 held `2479` of it, `96.3%`.
The max/min ratio was `177.07` against a configured ratio of `5`. Mean lag was `428.83`,
median `20`, and population standard deviation `916.87`.

The group was `Stable` with 1 member, and every partition was assigned. Rates are labeled
estimates over about 55.1 seconds: incoming `32.08` messages/second, processing `0`, lag
growth `32.08`.

On partition 3 the committed offset was `345` while lag was `2479`. The committed offsets on
the other partitions were `7`, `2`, `2`, `4`, and `11`. The tool's wording is: "This shows
no progress. It does not show that the process crashed." The handler was still running during
the sample; the scenario log shows partition 3 offsets continuing past this snapshot.

`partialFailures` is empty. Change correlation is `not_configured`. One earlier partition
hotspot incident matched with overlap score `0.85`; similarity is group, topic, and
finding-type overlap, not a model judgment.

## Sample finding

```json
{
  "type": "PARTITION_IMBALANCE",
  "severity": "high",
  "confidence": 0.95,
  "evidence": [
    "Known lag across 6 partitions: total 2573, min 14, max 2479, mean 428.83, median 20.",
    "Population standard deviation of known lag is 916.87.",
    "Max/min known lag ratio is 177.07. The configured PARTITION_IMBALANCE_RATIO is 5.",
    "Partition 3 lag is 2479, which is 96.3% of known lag."
  ],
  "recommendations": [
    "Compare message keys for the outlier partitions with the others.",
    "Inspect handler time and downstream calls on the outlier partitions.",
    "Confirm the configured PARTITION_IMBALANCE_RATIO matches the skew you care about."
  ]
}
```
