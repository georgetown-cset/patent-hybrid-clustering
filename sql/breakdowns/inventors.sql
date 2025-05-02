-- Find top inventors by cluster

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

-- number of patent families in clust
clust_size AS (
  SELECT
    cluster_id,
    COUNT(DISTINCT family_id) AS NPF
  FROM
    clusters
  GROUP BY
    cluster_id
),

-- merge inventors and cats
inventors AS (
  SELECT
    patent_id,
    family_id,
    cluster_id,
    inventor
  FROM
    clusters
  LEFT JOIN (
    SELECT
      patent_id,
      inventor
    FROM
      unified_patents.inventors
    )
    USING
      (patent_id)
),

-- Aggregate.
inventor_rank_tab AS (
  SELECT
    cluster_id,
    inventor,
    COUNT(DISTINCT family_id) AS NPF_inventor,
    ROW_NUMBER() OVER (PARTITION BY cluster_id ORDER BY COUNT(DISTINCT family_id) DESC) AS inventor_rank
  FROM (
    SELECT
      patent_id,
      family_id,
      cluster_id,
      inventor
    FROM
      inventors
    )
  WHERE
    inventor IS NOT NULL
  GROUP BY
    cluster_id,
    inventor
),

-- Get top 10 inventors.
top10_tab AS (
  SELECT
    cluster_id,
    COUNT(DISTINCT family_id) AS NPF_top10_inventors
  FROM
    inventors
  INNER JOIN (
    SELECT
      *
    FROM
      inventor_rank_tab
    WHERE
      inventor_rank <= 10
    )
    USING
      (cluster_id,
        inventor)
  GROUP BY
    cluster_id
),

-- Get missing inventors
miss_inventor_tab AS (
  SELECT
    cluster_id,
    SUM(miss_inventor) AS NPF_missing_all_inventors
  FROM (
    SELECT
      cluster_id,
      family_id,
      MIN(
        IF(inventor IS NULL, 1, 0)) AS miss_inventor
    FROM
      inventors
    GROUP BY
      family_id,
      cluster_id
    )
  GROUP BY
    cluster_id
)

-- Get inventors
SELECT
  cluster_id,
  inventor,
  inventor_rank,
  NPF_inventor,
  NPF_top10_inventors,
  NPF_missing_all_inventors
FROM (
  SELECT
    *
  FROM (
    SELECT
      *
    FROM
      clust_size
    LEFT JOIN (
      SELECT
        *
      FROM
        inventor_rank_tab
      )
      USING
        (cluster_id)
    )
  LEFT JOIN
    top10_tab
    USING
      (cluster_id)
  )
LEFT JOIN
  miss_inventor_tab
  USING
    (cluster_id)
ORDER BY
  cluster_id,
  inventor_rank
