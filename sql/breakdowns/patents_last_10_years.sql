/* getting patents with priority dates from last 10 years, with 150-day buffer period
   10*365 + 150 = 3800*/

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

merged_date AS (
  SELECT
    patent_id,
    clusters.family_id,
    cluster_id,
    first_priority_date
  FROM
    clusters
  LEFT JOIN
    unified_patents.dates
    USING
      (patent_id)
)

SELECT
  patent_id,
  family_id,
  cluster_id,
  first_priority_date
FROM merged_date
WHERE DATE_DIFF(first_priority_date, DATE_SUB(CURRENT_DATE(), INTERVAL 3800 DAY), DAY) > 0
