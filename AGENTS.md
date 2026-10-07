# Kafka Inspector MCP — AI Agent Development Instructions

## 1. Project

You are building **Kafka Inspector MCP**, a production-quality Model Context Protocol server written in TypeScript/Node.js.

The purpose of this project is to give AI agents structured, evidence-based access to Kafka operational information so they can investigate Kafka problems such as:

- consumer lag
- partition imbalance
- stalled consumers
- insufficient consumer capacity
- consumer throughput problems
- consumer-group instability
- offset problems
- producer/consumer throughput imbalance

The central capability is:

```text
diagnose_consumer_group()
```

The system must not merely expose raw Kafka APIs. It must provide useful operational analysis while keeping the underlying evidence visible to the AI agent.

---

# 2. Core principle

MCP is the interface.

The diagnostic engine is the product.

Do not build a collection of thin KafkaJS wrappers and call it complete.

Bad:

```text
MCP tool
  ↓
KafkaJS
  ↓
return raw response
```

Preferred:

```text
MCP tool
  ↓
Application service
  ↓
Domain analysis
  ↓
Kafka adapter
  ↓
KafkaJS
  ↓
Structured evidence
```

---

# 3. Technology

Use:

- Node.js
- TypeScript
- @modelcontextprotocol/sdk
- KafkaJS
- Zod
- PostgreSQL
- Redis
- Pino
- Vitest
- Testcontainers
- Docker Compose

Use ES modules.

Use strict TypeScript.

Do not introduce unnecessary frameworks.

Do not use LangChain or LangGraph unless explicitly requested.

Do not turn this project into a generic AI agent framework.

---

# 4. Architecture

Use a modular architecture:

```text
src/
├── index.ts
├── server/
├── tools/
├── services/
├── domain/
├── schemas/
├── config/
├── errors/
└── utils/
```

Architecture layers:

```text
MCP Layer
    ↓
Application Services
    ↓
Domain / Diagnosis Engine
    ↓
Infrastructure Adapters
    ↓
Kafka / PostgreSQL / Redis
```

MCP tools must not contain business logic.

KafkaJS calls must not be scattered throughout MCP handlers.

---

# 5. Primary capabilities

The server should eventually support:

## Topic inspection

```text
list_topics()
get_topic_metadata()
get_partition_info()
```

## Consumer groups

```text
list_consumer_groups()
get_consumer_group()
get_consumer_assignments()
```

## Lag

```text
get_consumer_lag()
get_lag_history()
```

## Diagnosis

```text
analyze_consumer_lag()
analyze_partition_imbalance()
analyze_throughput()
detect_stalled_consumers()
diagnose_consumer_group()
```

## Historical context

```text
find_similar_incidents()
```

## Future integrations

```text
Prometheus
GitHub
deployment systems
OpenTelemetry
```

---

# 6. Read-only first

The initial system MUST be read-only.

Do not implement destructive Kafka operations in the initial milestones.

Do not implement:

```text
delete_topic
reset_offsets
alter_partitions
produce_message
stop_consumer
```

unless explicitly requested in a later milestone.

If write operations are eventually introduced, they must require explicit user confirmation and must never be automatically executed based solely on LLM reasoning.

---

# 7. Configuration

Use environment variables.

Example:

```env
NODE_ENV=development

KAFKA_BROKERS=localhost:9092
KAFKA_CLIENT_ID=kafka-inspector

KAFKA_SECURITY_PROTOCOL=PLAINTEXT

POSTGRES_URL=postgresql://postgres:postgres@localhost:5432/kafka_inspector

REDIS_URL=redis://localhost:6379

LOG_LEVEL=info

PARTITION_IMBALANCE_RATIO=5
STALL_WINDOW_SECONDS=300
```

Create:

```text
.env.example
```

Never commit secrets.

---

# 8. Kafka abstraction

Create dedicated services:

```text
KafkaClient
AdminService
MetadataService
ConsumerGroupsService
OffsetsService
```

All KafkaJS interaction should be isolated behind these services.

The rest of the application should not depend directly on KafkaJS types wherever practical.

---

# 9. Lag calculation

Lag must be calculated per partition.

Conceptually:

```text
lag = highWatermark - committedOffset
```

Total lag:

```text
totalLag = sum(partitionLag)
```

Correctly handle:

- missing committed offsets
- new consumer groups
- empty topics
- transient Kafka errors
- rebalance states
- deleted partitions
- invalid offsets

Never silently convert an unknown value into zero.

Distinguish:

```text
0 lag
```

from:

```text
unknown lag
```

---

# 10. Diagnosis engine

Create a dedicated:

```text
DiagnosisEngine
```

It must use deterministic analysis before any LLM reasoning.

Supported findings should include:

```text
CONSUMER_STALLED
CONSUMER_SLOW
PARTITION_IMBALANCE
INSUFFICIENT_CONSUMER_CAPACITY
PRODUCER_OUTPACING_CONSUMER
REBALANCING
NO_COMMITTED_OFFSET
UNKNOWN
```

Each finding should contain:

```text
type
severity
confidence
evidence
recommendations
```

Example:

```json
{
  "type": "PARTITION_IMBALANCE",
  "severity": "high",
  "confidence": 0.91,
  "evidence": ["Partition 7 contains 96.7% of total lag"],
  "recommendations": ["Investigate key distribution", "Inspect consumer processing for partition 7"]
}
```

Never present speculation as fact.

Use language such as:

```text
likely
possible
evidence suggests
insufficient evidence
```

when certainty is not justified.

---

# 11. Partition imbalance

Analyze:

- total lag per partition
- lag share
- mean
- median
- standard deviation
- max/min relationship
- outlier partitions

Thresholds must be configurable.

Do not hardcode arbitrary production assumptions.

---

# 12. Consumer stall detection

Track consumer progress over time.

Distinguish:

```text
consumer not assigned
consumer assigned but not progressing
consumer progressing slowly
consumer actively processing
```

A consumer that has not progressed must not automatically be classified as crashed.

Provide evidence.

---

# 13. Throughput analysis

Estimate:

```text
incoming rate
consumer processing rate
lag growth rate
```

If:

```text
incoming rate > processing rate
```

identify that as a potential reason for continuously increasing lag.

Do not claim exact producer throughput unless the available Kafka/metrics data supports it.

Clearly label estimates.

---

# 14. Historical observations

Persist lag observations in PostgreSQL.

Suggested schema:

```text
lag_observations

id
cluster_id
topic
consumer_group
partition
high_watermark
committed_offset
lag
observed_at
```

Add indexes for:

```text
topic + consumer_group + partition + observed_at
```

Use parameterized SQL.

Do not construct SQL using user-provided strings.

---

# 15. Redis

Redis may be used for:

- short-lived cached metadata
- recent observations
- distributed locks if later required
- rate limiting if later required

Do not make Redis the source of truth for historical data.

PostgreSQL is the durable source of truth.

---

# 16. MCP tool design

Tools must have:

- clear names
- precise descriptions
- Zod input schemas
- bounded responses
- predictable output
- useful error messages

Do not expose enormous Kafka payloads to the LLM.

Prefer:

```text
summary
metrics
findings
evidence
```

over raw broker metadata.

---

# 17. Main diagnostic workflow

The expected workflow is:

```text
diagnose_consumer_group()
        ↓
topic metadata
        ↓
consumer group state
        ↓
partition assignments
        ↓
current lag
        ↓
historical lag
        ↓
partition analysis
        ↓
consumer progress
        ↓
throughput analysis
        ↓
diagnosis engine
        ↓
findings
        ↓
evidence
        ↓
recommendations
```

The MCP server should perform this aggregation internally where appropriate instead of forcing the AI agent to manually call ten tools for every basic diagnosis.

---

# 18. Evidence-first output

A diagnosis must always expose supporting evidence.

Bad:

```text
The consumer is slow.
```

Good:

```text
Finding:
CONSUMER_SLOW

Evidence:
- Partition 7 lag increased from 2,100 to 92,000
- Consumer assigned to partition 7 processed only 320 messages
  during the observation window
- Incoming rate exceeded processing rate

Confidence:
0.86
```

The AI agent should be able to inspect the evidence behind every finding.

---

# 19. MCP resources

Eventually provide resources:

```text
kafka://topics
kafka://topic/{topic}
kafka://consumer-groups
kafka://consumer-group/{group}
kafka://consumer-group/{group}/lag
kafka://incidents/{id}
```

Resources should provide contextual state.

Tools should perform queries and analysis.

---

# 20. MCP prompts

Eventually provide prompts:

```text
diagnose_kafka_lag
investigate_consumer_group
analyze_partition_imbalance
explain_consumer_group
```

The prompts should guide the AI toward evidence-based investigation.

Do not hardcode a conclusion into prompts.

---

# 21. Testing requirements

Every domain calculation requires unit tests.

At minimum test:

```text
lag calculation
total lag
lag growth
partition imbalance
consumer stall
throughput
diagnosis ranking
missing offsets
empty topics
rebalance state
Kafka failures
```

Use Testcontainers for Kafka integration tests.

Test MCP tools independently from KafkaJS where practical.

---

# 22. Failure handling

Handle:

- Kafka unavailable
- broker timeout
- authentication failure
- authorization failure
- topic not found
- consumer group not found
- rebalance
- partial metadata failure
- PostgreSQL unavailable
- Redis unavailable

Errors must be structured.

Do not leak credentials or connection strings.

Do not crash the MCP process because one optional diagnostic source failed.

---

# 23. Observability

The MCP server itself should have:

- structured logs
- request/tool execution timing
- error counters
- Kafka request timing
- diagnosis timing

Later expose Prometheus metrics.

Suggested metrics:

```text
mcp_tool_calls_total
mcp_tool_errors_total
kafka_request_duration_seconds
diagnosis_duration_seconds
lag_observations_total
```

---

# 24. Demo environment

Create a reproducible Docker Compose environment containing:

```text
Kafka
PostgreSQL
Redis
Kafka Inspector MCP
Demo Producer
Demo Consumer
```

Create failure scenarios:

```text
slow-consumer
stalled-consumer
partition-hotspot
producer-spike
```

The demo should allow:

```bash
npm run demo:slow-consumer
npm run demo:partition-hotspot
npm run demo:producer-spike
```

Then an AI agent can investigate the generated problem.

---

# 25. Development methodology

Implement one milestone at a time.

Before starting a milestone:

1. Inspect the existing code.
2. Read AGENTS.md.
3. Understand the current architecture.
4. Identify the smallest required change.
5. Implement.
6. Write tests.
7. Run tests.
8. Run lint.
9. Run TypeScript build.
10. Update documentation.
11. Report what changed.

Do not jump ahead to future milestones.

Do not rewrite working code without a reason.

Do not add speculative abstractions.

---

# 26. Definition of done for every milestone

A milestone is complete only when:

- implementation exists
- tests exist
- tests pass
- TypeScript compiles
- lint passes
- error handling exists
- documentation is updated
- no secrets are committed
- existing functionality still works

---

# 27. Milestone order

Implement in this exact order:

```text
M0  Project foundation
M1  Kafka connectivity
M2  Topic inspection
M3  Consumer groups
M4  Consumer lag
M5  Lag history
M6  Diagnosis engine
M7  Partition imbalance
M8  Consumer stall detection
M9  Throughput analysis
M10 diagnose_consumer_group
M11 RAG / similar incidents
M12 Prometheus
M13 GitHub / deployment correlation
M14 Safety boundaries
M15 MCP resources
M16 MCP prompts
M17 Testing hardening
M18 Chaos/demo environment
M19 Docker Compose
M20 Documentation and release
```

---

# 28. What not to build

Do not turn this into:

- a Kafka management UI
- a generic chatbot
- a generic RAG application
- a generic coding agent
- a Kafka admin replacement
- a LangChain framework
- a Kubernetes operator
- a microservices architecture for no reason

The product is:

> **An MCP-powered Kafka investigation and diagnosis system for AI agents.**

Keep the scope centered around that problem.

---

# 29. Final success criteria

The project should eventually support this interaction:

User:

> "Why is payment-worker consumer lag growing on payment-events?"

AI agent uses:

```text
diagnose_consumer_group()
```

The system gathers:

```text
topic metadata
consumer group state
partition assignments
current lag
lag history
consumer progress
throughput
partition distribution
historical incidents
```

The response should contain:

```text
Current status

Impact

Timeline

Findings

Evidence

Likely causes

Confidence

Recommended investigation steps

Related historical incidents
```

The system must distinguish:

```text
observed fact
derived metric
inference
recommendation
```

Do not blur these categories.

---

# 30. Engineering philosophy

Prefer:

```text
simple
observable
testable
evidence-based
read-only
modular
```

over:

```text
clever
over-engineered
AI-heavy
opaque
```

The LLM should perform reasoning.

The MCP server should provide **high-quality structured evidence and deterministic analysis**.

That separation is fundamental to this project.
