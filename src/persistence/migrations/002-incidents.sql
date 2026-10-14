-- M11 similar incidents. Kept identical to INCIDENTS_DDL in
-- src/persistence/incident-store.ts (a unit test enforces they never drift).
CREATE TABLE IF NOT EXISTS incidents (
  id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  cluster_id TEXT NOT NULL,
  topic TEXT NOT NULL,
  consumer_group TEXT NOT NULL,
  finding_types TEXT[] NOT NULL,
  severity TEXT NOT NULL,
  summary TEXT NOT NULL,
  evidence JSONB NOT NULL,
  observed_at TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS incidents_topic_group_time
  ON incidents (topic, consumer_group, observed_at);
