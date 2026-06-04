-- Find percentage semiconductors by cluster

-- Get dummy families
WITH
families_with_dummies AS (
  SELECT
    patent_id,
    COALESCE(family_id,
      "X-" || patent_id) AS family_id
  FROM
    unified_patents.links
),

-- Get patent clusters, with all patent ids in the families
clusters AS (
  SELECT DISTINCT
    patent_id,
    family_id,
    cluster_assignment.cluster_id
  FROM
    staging_patent_clusters.cluster_assignment
  LEFT JOIN
    families_with_dummies
    USING
      (family_id)
),

/* Semiconductors prediction */
semiconductors_tab AS (
  SELECT
    patent_id,
    family_id,
    -- If the patent is in the semiconductor table, it's semiconductors
    1 AS semiconductors
  FROM
    unified_patents.semiconductor_patents
),

/* Merge clusters and semiconductor predictions, including each only once per family id */
merged AS (
  SELECT DISTINCT
    clusters.family_id,
    cluster_id,
    COALESCE(semiconductors, 0) AS semiconductors
  FROM
    clusters
  LEFT JOIN
    semiconductors_tab
    USING
      (patent_id)
)

SELECT
  cluster_id,
  SUM(semiconductors) / NULLIF(COUNT(family_id), 0) AS pred_semiconductors
FROM
  merged
GROUP BY
  cluster_id
ORDER BY
  cluster_id
