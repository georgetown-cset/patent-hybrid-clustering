CREATE OR REPLACE TABLE staging_patent_clusters.hybrid_sts_scaled_weights_pruned AS (
SELECT
  node_id AS id,
  ref_id,
  weight
FROM staging_patent_clusters.hybrid_sts_scaled_weights_link_counts
WHERE link_count > 1
)
