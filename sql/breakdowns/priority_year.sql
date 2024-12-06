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

-- get priority date and age (now is 0 years)
patent_dates AS (
  SELECT
    EXTRACT(year
    FROM
      first_priority_date) AS year,
    patent_id,
    family_id
  FROM
    unified_patents.dates
),

merged_yr AS (
  SELECT
    patent_id,
    clusters.family_id,
    cluster_id,
    year
  FROM
    clusters
  LEFT JOIN
    patent_dates
  USING
    (patent_id)
)

SELECT DISTINCT
  cluster_id,
  COUNT(family_id) AS NPF_cluster,
  year
FROM
  merged_yr
WHERE
  year > EXTRACT(YEAR FROM CURRENT_DATE()) - 15
GROUP BY
  year,
  cluster_id
ORDER BY
  cluster_id,
  year