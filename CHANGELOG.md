# Changelog

## 0.2.0

Diagnosis and the rest of the read-only investigation surface.

- Diagnosis engine with ranked findings, evidence categories, and configurable imbalance and stall windows.
- Partition imbalance, consumer progress, and throughput estimates.
- `diagnose_consumer_group` plus the focused analysis tools.
- Incident history and `find_similar_incidents`.
- Optional Redis metadata cache, Prometheus metrics, and GitHub or deployment correlation.
- Read-only tool allowlist. Destructive Kafka operations are rejected.
- MCP resources and investigation prompts.
- Docker Compose stack and demo scenarios: slow consumer, stalled consumer, partition hotspot, and producer spike.

## 0.1.0

Foundation through lag history: Kafka connectivity, topic inspection, consumer groups, per-partition lag, and PostgreSQL lag history.
