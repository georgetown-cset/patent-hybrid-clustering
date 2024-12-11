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

new_pat AS (
  -- assume 150 DAYS lag between expected publication and expected import date
  -- include any patents from the last 10 years
  -- (in papers we do 5 but patents have a longer lag)
  SELECT
    *
  FROM
    clusters
  INNER JOIN (
    SELECT
      patent_id,
      first_priority_date,
      EXTRACT(YEAR FROM CURRENT_DATE()) - EXTRACT(YEAR FROM first_priority_date) AS age
    FROM
      unified_patents.dates
    WHERE
      DATE_DIFF(first_priority_date, DATE_SUB(CURRENT_DATE(), INTERVAL 3800 DAY), DAY) > 0)
    USING
      (patent_id)
),

-- Link citations to dummy families so we can count all patents and not ignore null families
cit AS (
  SELECT
    patent_id,
    clusters.family_id,
    patent_reference
  FROM
    clusters
  LEFT JOIN
    unified_patents.references
    USING
      (patent_id)
),

-- get citations of new patent families
new_cit AS (
  SELECT
    COUNT(DISTINCT family_id) AS Ncit,
    ref_id AS id
  FROM
    cit
  LEFT JOIN (
    SELECT
      patent_id,
      clusters.family_id AS ref_id
    FROM
      unified_patents.links
    LEFT JOIN
      clusters
      USING
        (patent_id)
    ) AS fam
    ON
      fam.patent_id = patent_reference
  WHERE
    patent_reference IN (
      SELECT patent_id
      FROM
        new_pat
    )
  GROUP BY
    ref_id
),

-- link citations and clusters
cit_clust AS (
  SELECT
    *
  FROM
    clusters
  INNER JOIN
    new_cit
    ON
      clusters.family_id = new_cit.id
),

-- Get initial title and date info
title_date_info AS (
  SELECT
    patent_id,
    clusters.family_id,
    title,
    title_original,
    IF(priority_year IS NULL, '', CAST(priority_year AS STRING)) AS priority_year,
    patent_date
  FROM
    unified_patents.metadata
  LEFT JOIN (
    SELECT
      patent_id,
      family_id,
      EXTRACT(YEAR
        FROM
        first_priority_date) AS priority_year,
      COALESCE(first_priority_date, application_date, publication_date) AS patent_date
    FROM
      unified_patents.dates
  )
  USING
    (patent_id)
  LEFT JOIN
    clusters
    USING
      (patent_id)
  WHERE title IS NOT NULL
    OR title_original IS NOT NULL
),

get_ordering AS (
  SELECT
    family_id,
    patent_id,
    title,
    title_original,
    priority_year,
    patent_date,
    RANK() OVER (
      PARTITION BY family_id ORDER BY patent_date ASC, LENGTH(COALESCE(title, title_original)) DESC
    ) AS ordering
  FROM
    title_date_info
),

-- Group title and year by family
family_title_date AS (
  SELECT
    family_id,
    COALESCE(title, title_original) AS title,
    priority_year
  FROM
    get_ordering
  WHERE ordering = 1
),

-- get distinct cited patents
cit_pat AS (
  SELECT DISTINCT
    cluster_id,
    id AS family_id,
    Ncit AS citations
  FROM
    cit_clust
  WHERE
    Ncit IS NOT NULL
),

-- rank them
cit_rank AS (
  SELECT DISTINCT
    cluster_id,
    family_id,
    citations,
    ROW_NUMBER() OVER (PARTITION BY cluster_id ORDER BY citations DESC) AS citation_rank
  FROM
    cit_pat
  WHERE
    citations IS NOT NULL
)

SELECT DISTINCT
  family_id,
  cluster_id,
  citations,
  citation_rank,
  title AS most_cited_title
FROM
  cit_rank
LEFT JOIN
  family_title_date
  USING
    (family_id)
ORDER BY
  cluster_id,
  citation_rank
