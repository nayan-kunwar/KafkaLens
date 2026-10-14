# Partition hotspot investigation

Captured on 2026-10-14 from the local Compose stack. The payload in
[partition-hotspot-diagnosis.json](partition-hotspot-diagnosis.json) is the second
`diagnose_consumer_group` response, unchanged except for pretty-printing.

## How it was produced

`demo/partition-hotspot.ts` created topic `hotspot-events` with 6 partitions and group
`hotspot-consumer`. About 95% of records use the key `hot-key`. The handler waits 80ms.
The run lasted 120 seconds. The consumer joined at `2026-10-14T17:56:45.543Z` and the loop
stopped at `2026-10-14T17:58:45.539Z` after 1,292 handler calls.

The inspector called `diagnose_consumer_group` with `groupId=hotspot-consumer` and
`topic=hotspot-events`.

| Setting                     | Value            |
| --------------------------- | ---------------- |
| `KAFKA_BROKERS`             | `localhost:9092` |
| `STALL_WINDOW_SECONDS`      | `60`             |
| `PARTITION_IMBALANCE_RATIO` | `5`              |

Host ports for this stack were Kafka `9092`, Postgres `5434`, and Redis `6380`. Ports `5432`
and `6379` were already in use by other local services, so this stack was not bound there.

The two calls were at `2026-10-14T17:57:27.707Z` and `2026-10-14T17:58:23.624Z`. Stored history
used by the second call runs from `2026-10-14T17:57:28.438Z` to `2026-10-14T17:58:24.363Z`
(55.9 seconds, 18 samples). `partialFailures` is empty. Change correlation is `not_configured`.

## What the tool returned

`status` is `attention`. Top confidence is `0.95`.

| Finding                          | Severity | Confidence |
| -------------------------------- | -------- | ---------- |
| `PARTITION_IMBALANCE`            | high     | 0.95       |
| `CONSUMER_STALLED`               | high     | 0.84       |
| `PRODUCER_OUTPACING_CONSUMER`    | high     | 0.78       |
| `INSUFFICIENT_CONSUMER_CAPACITY` | medium   | 0.58       |

Known lag was `8959` across all 6 partitions. Partition 3 held `8593` of it, `95.9%`. The
max/min ratio was `330.50` against a configured ratio of `5`. The group was `Stable` with 1
member, and every partition was assigned.

Rates are labeled estimates, over about 55.9 seconds: incoming `32.186` messages/second,
processing `0`, lag growth `32.186`. The tool describes that as a possible reason for the
increase, and it says the rates are not broker producer metrics.

On partition 3 the committed offset stayed at `3665` while lag was `8593`. The same was true
on the other five partitions. The tool's wording is: "This shows no progress. It does not show
that the process crashed." During that same run the handler was still being invoked. Its log
prints `message.offset` (the record being handled), and those record offsets on partition 3
continued through `4899` before the loop stopped at `17:58:45Z`. The stall finding is about
the committed offset the broker returned.

`INSUFFICIENT_CONSUMER_CAPACITY` is marked as a capacity hypothesis: 1 member and 6 partitions,
with estimated lag growth.

One earlier incident in this same session matched. Id `1`, observed at
`2026-10-14T17:47:58.888Z`, finding `PARTITION_IMBALANCE`, overlap score `0.85`. Similarity is
group, topic, and finding-type overlap.

## Sample finding

```json
{
  "type": "PARTITION_IMBALANCE",
  "severity": "high",
  "confidence": 0.95,
  "evidence": [
    "Known lag across 6 partitions: total 8959, min 26, max 8593, mean 1493.16, median 85.",
    "Population standard deviation of known lag is 3175.22.",
    "Max/min known lag ratio is 330.50. The configured PARTITION_IMBALANCE_RATIO is 5.",
    "Partition 3 lag is 8593, which is 95.9% of known lag."
  ],
  "recommendations": [
    "Compare message keys for the outlier partitions with the others.",
    "Inspect handler time and downstream calls on the outlier partitions.",
    "Confirm the configured PARTITION_IMBALANCE_RATIO matches the skew you care about."
  ]
}
```
