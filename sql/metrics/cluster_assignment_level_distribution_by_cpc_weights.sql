-- Finding histograms of patent corpus average cluster assignment levels
CREATE OR REPLACE TABLE patent_clustering_metrics.cluster_assignment_level_distribution_by_cpc_weights AS (
  WITH
  -- Finding cluster assignment levels using the average of patent in each
  cluster_assignment_levels AS (
    SELECT
      cluster_id,
      AVG(assignment_level) AS cluster_assignment_level
    FROM patent_clustering_metrics.patent_assignment_level_by_cpc_weights
    GROUP BY cluster_id
  )

-- Histogram of average distribution
SELECT
  FLOOR(cluster_assignment_level * 25)/25 AS bin_floor,
  COUNT(cluster_id) AS n_new_clusters
FROM cluster_assignment_levels
GROUP BY bin_floor
ORDER BY bin_floor

)