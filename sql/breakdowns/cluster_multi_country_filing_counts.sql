-- calculates number of patents in a cluster with at least one cross-filing,
-- and total number of patents over last 10 years
WITH
country_counts AS (
  SELECT
    family_id,
    COUNT(DISTINCT publication_country) AS country_count
  FROM
    unified_patents.metadata
  GROUP BY
    family_id
)

SELECT
  cluster_id,
  COUNTIF(country_count > 1) AS multi_country_count,
  COUNT(family_id) AS ten_year_count
FROM
  staging_patent_clusters.cluster_assignment
LEFT JOIN
  country_counts
  USING
    (family_id)
WHERE
  family_id IN (
    SELECT family_id
    FROM
      staging_patent_clusters.patents_last_10_years)
GROUP BY
  cluster_id
