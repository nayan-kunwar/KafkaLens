-- M3/M8: cluster-scoped lookups and retention support.
-- History, incident similarity, and dedupe filter by cluster_id; retention
-- deletes by observed_at. These indexes keep both paths off a full scan.
CREATE INDEX IF NOT EXISTS lag_observations_cluster_topic_group_partition_time
  ON lag_observations (cluster_id, topic, consumer_group, partition, observed_at);
CREATE INDEX IF NOT EXISTS lag_observations_observed_at
  ON lag_observations (observed_at);
CREATE INDEX IF NOT EXISTS incidents_cluster_topic_group_time
  ON incidents (cluster_id, topic, consumer_group, observed_at);
CREATE INDEX IF NOT EXISTS incidents_observed_at
  ON incidents (observed_at);
