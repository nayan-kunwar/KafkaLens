-- M5 lag history. Kept identical to LAG_OBSERVATIONS_DDL in
-- src/persistence/lag-store.ts (a unit test enforces they never drift).
-- Unknown values are stored as NULL, never as zero.
CREATE TABLE IF NOT EXISTS lag_observations (
  id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  cluster_id TEXT NOT NULL,
  topic TEXT NOT NULL,
  consumer_group TEXT NOT NULL,
  partition INTEGER NOT NULL,
  high_watermark TEXT,
  committed_offset TEXT,
  lag TEXT,
  observed_at TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS lag_observations_topic_group_partition_time
  ON lag_observations (topic, consumer_group, partition, observed_at);
