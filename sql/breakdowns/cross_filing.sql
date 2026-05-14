-- gets yearly country cross-filing counts for each cluster

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

last_ten AS (
  SELECT DISTINCT
    family_id,
    EXTRACT(YEAR FROM first_priority_date) AS year
  FROM
    staging_patent_clusters.patents_last_10_years
),

-- Get patent clusters, with all patent ids in the families
clusters AS (
  SELECT DISTINCT
    patent_id,
    family_id,
    cluster_assignment.cluster_id,
    year
  FROM
    staging_patent_clusters.cluster_assignment
  INNER JOIN
    last_ten
    USING
      (family_id)
  LEFT JOIN
    families_with_dummies
    USING
      (family_id)
),

single_country_patents AS (
  SELECT
    clusters.family_id,
    COUNT(DISTINCT(publication_country)) AS country_count
  FROM
    clusters
  INNER JOIN
    unified_patents.metadata
    USING
      (patent_id)
  GROUP BY
    family_id
),

cross_filings AS (
  SELECT
    cluster_id,
    priority_country_name AS country_a,
    publication_country_name AS country_b,
    year,
    COUNT(DISTINCT metadata.patent_id) AS num_patents
  FROM
    unified_patents.priority_country
  INNER JOIN
    clusters
    USING
      (family_id)
  INNER JOIN
    unified_patents.metadata
    USING
      (family_id)
  WHERE
    (family_id IN (
      SELECT family_id
      FROM
        last_ten))
    AND ((priority_country_name != publication_country_name)
      OR (family_id IN (
        SELECT family_id
        FROM
          single_country_patents
        WHERE
          country_count = 1)))
  GROUP BY
    cluster_id,
    priority_country_name,
    publication_country_name,
    year
)

SELECT
  cluster_id,
  country_a,
  ARRAY_AGG(STRUCT(country_b,
      num_patents,
      year)
    ORDER BY
      num_patents DESC) AS cross_filings_w_counts
FROM
  cross_filings
GROUP BY
  country_a,
  cluster_id
