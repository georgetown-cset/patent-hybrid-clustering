-- Finding distribution of patent assignments for new and old clustering

CREATE OR REPLACE TABLE patent_clustering_metrics.patent_assignment_level_distribution_comparison_by_text_weights AS (
  WITH
  -- Counting patent assignment levels for each of the patents in the old clustering
  old_level_distribution AS (
    SELECT
      assignment_level,
      COUNT(DISTINCT family_id) AS n_patents_old_clustering
    FROM patent_clustering_metrics.old_clusters_patent_assignment_level_by_text_weights
    GROUP BY assignment_level
  ),

  -- Counting patent assignment levels for each of the patents in the new clustering
  new_level_distribution AS (
    SELECT
      assignment_level,
      COUNT(DISTINCT family_id) AS n_patents_new_clustering
    FROM patent_clustering_metrics.new_clusters_patent_assignment_level_by_text_weights
    GROUP BY assignment_level
  )

  -- Finding the total distribution for each of the old and new clustering
  SELECT
    assignment_level,
    n_patents_old_clustering,
    n_patents_new_clustering
  FROM old_level_distribution
  FULL JOIN new_level_distribution USING (assignment_level)
)
