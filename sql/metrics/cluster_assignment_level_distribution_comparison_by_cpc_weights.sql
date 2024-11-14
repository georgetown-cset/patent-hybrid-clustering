-- Finding histograms of old and new corpus average cluster assignment levels
CREATE OR REPLACE TABLE patent_clustering_metrics.cluster_assignment_level_distribution_comparison_by_cpc_weights AS (
  WITH
  -- Finding old cluster assignment levels using the average of papers in each
  old_cluster_assignment_levels AS (
    SELECT
      cluster_id,
      AVG(assignment_level) AS cluster_assignment_level
    FROM patent_clustering_metrics.old_clusters_patent_assignment_level_by_cpc_weights
    GROUP BY cluster_id
  ),

  -- Finding new cluster assignment levels using the average of papers in each
  new_cluster_assignment_levels AS (
    SELECT
      cluster_id,
      AVG(assignment_level) AS cluster_assignment_level
    FROM patent_clustering_metrics.new_clusters_patent_assignment_level_by_cpc_weights
    GROUP BY cluster_id
  ),

  -- Histogram of old average distribution
  old_histogram AS (
    SELECT
      FLOOR(cluster_assignment_level * 25) / 25 AS bin_floor,
      COUNT(cluster_id) AS n_old_clusters
    FROM old_cluster_assignment_levels
    GROUP BY bin_floor
  ),

  -- Histogram of new average distribution
  new_histogram AS (
    SELECT
      FLOOR(cluster_assignment_level * 25) / 25 AS bin_floor,
      COUNT(cluster_id) AS n_new_clusters
    FROM new_cluster_assignment_levels
    GROUP BY bin_floor
  )

  SELECT
    *
  FROM old_histogram
  FULL JOIN new_histogram
    USING (bin_floor)
  ORDER BY bin_floor
)
