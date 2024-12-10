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

-- merge priority countries and clusters
priority_countries AS (
  SELECT
    patent_id,
    family_id,
    cluster_id,
    priority_country
  FROM
    clusters
  LEFT JOIN (
    SELECT
      patent_id,
      priority_country
    FROM
      staging_unified_patents.priority_country)
    USING
      (patent_id)
),

-- Get whole names
full_pri_country AS (
  SELECT
    patent_id,
    family_id,
    cluster_id,
    standard_name AS country
  FROM
    priority_countries
  LEFT JOIN (
    SELECT
      standard_name,
      raw_alpha_2
    FROM
      countries.country_code)
    ON
      priority_country.priority_country = raw_alpha_2
),

-- Aggregate.
country_rank_tab AS (
  SELECT
    cluster_id,
    country,
    COUNT(DISTINCT family_id) AS NPF_country,
    ROW_NUMBER() OVER (PARTITION BY cluster_id ORDER BY COUNT(DISTINCT family_id) DESC) AS country_rank
  FROM (
    SELECT
      patent_id,
      family_id,
      cluster_id,
      country
    FROM
      full_pri_country)
  WHERE
    country IS NOT NULL
  GROUP BY
    cluster_id,
    country
),

-- get top 10 priority countries
top10_tab AS (
  SELECT
    cluster_id,
    COUNT(DISTINCT family_id) AS NPF_top10_countries
  FROM
    full_pri_country
  INNER JOIN (
    SELECT
      *
    FROM
      country_rank_tab
    WHERE
      country_rank <= 10 )
    USING
      (cluster_id,
        country)
  GROUP BY
    cluster_id
),

-- get missing countries
miss_country_tab AS (
  SELECT
    cluster_id,
    SUM(miss_org) AS NPF_missing_all_countries
  FROM (
    SELECT
      cluster_id,
      family_id,
      MIN(
        IF(country IS NULL,
          1,
          0)) AS miss_org
    FROM
      full_pri_country
    GROUP BY
      family_id,
      cluster_id )
  GROUP BY
    cluster_id
)

-- Get countries
SELECT
  cluster_id,
  country,
  country_rank,
  NPF_country,
  NPF_top10_countries,
  NPF_missing_all_countries
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
        country_rank_tab
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
  miss_country_tab
  USING
    (cluster_id)
ORDER BY
  cluster_id,
  country_rank
