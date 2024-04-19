CREATE OR REPLACE TABLE staging_patent_clusters.patents_to_translate AS (
  SELECT
    *
  FROM `staging_patent_clusters.UP_to_translate`

  UNION ALL

  SELECT
    *
  FROM `staging_patent_clusters.LID_to_translate`
)
