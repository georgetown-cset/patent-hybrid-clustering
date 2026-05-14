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

cross_filings AS (
  SELECT DISTINCT
    cluster_id,
    family_id,
    priority_country_name AS country_a,
    publication_country_name AS country_b,
    year
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
    AND ((priority_country_name != publication_country_name))
),

has_cross_filing AS (
  SELECT
    cluster_id,
    COUNT(DISTINCT family_id) AS cross_filing_count
  FROM
    cross_filings
  GROUP BY
    cluster_id
)

SELECT
  cluster_id,
  COALESCE(cross_filing_count, 0) AS cross_filing_count,
  NPF,
  cross_filing_count / NPF AS cross_filing_percent,
  PERCENT_RANK() OVER (ORDER BY cross_filing_count / NPF) AS cross_filing_percentile
FROM
  clust_size
LEFT JOIN
  has_cross_filing
  USING (cluster_id)
