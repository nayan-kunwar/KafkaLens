# Diagnosis output

`diagnose_consumer_group` is the investigation entry point. It reads Kafka, stored lag history, and optional incident and change data, then runs deterministic checks. The model does the reasoning. This server does not.

## Categories

Every statement in `evidence` has a `kind`:

| kind             | Meaning                                                      |
| ---------------- | ------------------------------------------------------------ |
| `observed_fact`  | Read from Kafka or from stored history                       |
| `derived_metric` | Computed from those values. Rates are estimates              |
| `inference`      | A finding from the rules below. Not proof of a root cause    |
| `recommendation` | A next investigation step. Not an action the server will run |

`likelyCauses` repeats the inferences. `recommendedSteps` repeats the recommendations. `confidence` is the top finding's confidence, or null when there is no finding.

Unknown lag is null. It is never stored or summed as zero.

## Findings

| Type                             | When it is raised                                                                                                                               |
| -------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------- |
| `REBALANCING`                    | Group state is `PreparingRebalance` or `CompletingRebalance`. Stall and slowness findings are withheld in that state.                           |
| `NO_COMMITTED_OFFSET`            | High watermark is above 0 and there is no committed offset.                                                                                     |
| `INSUFFICIENT_CONSUMER_CAPACITY` | No members while known lag is above 0, or lag is growing with fewer members than partitions. The second case is explicitly a hypothesis.        |
| `PARTITION_IMBALANCE`            | At least two known lags, and a partition is at least `PARTITION_IMBALANCE_RATIO` times the minimum (or the mean, when the minimum is 0).        |
| `CONSUMER_STALLED`               | An assigned partition's committed offset did not move across at least 80% of `STALL_WINDOW_SECONDS` while lag was above 0. This is not a crash. |
| `CONSUMER_SLOW`                  | The committed offset advanced and lag still grew.                                                                                               |
| `PRODUCER_OUTPACING_CONSUMER`    | Estimated incoming rate is above estimated processing rate and estimated lag growth is positive.                                                |
| `UNKNOWN`                        | Lag or offset data exists, and none of the rules above applied.                                                                                 |

Findings are ordered by severity, then confidence, then type.

## What fails open

PostgreSQL, Redis, GitHub, and the deployment feed are optional. A failure is a `partialFailures` entry or a status of `unavailable` / `not_configured`. The live Kafka read still has to succeed. Topic and group `NOT_FOUND` still fail the call.

Incident rows are written only for findings at severity medium or higher, and an identical fingerprint inside 15 minutes is not inserted again. Similarity is group, topic, and finding-type overlap. There is no embedding model.

GitHub commits and deployments, and `DEPLOYMENT_SOURCE_URL`, are nearby changes. The report says they are not causes.
