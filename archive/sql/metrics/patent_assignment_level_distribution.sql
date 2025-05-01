-- Finding distribution of patent assignments for corpus

CREATE OR REPLACE TABLE patent_clustering_metrics.patent_assignment_level_distribution AS (
  -- Counting patent assignment levels for each of the patents in the new corpus
  SELECT
    assignment_level,
    COUNT(DISTINCT family_id) AS n_patents_new_corpus
  FROM patent_clustering_metrics.patent_assignment_level
  GROUP BY assignment_level
  ORDER BY assignment_level
)
