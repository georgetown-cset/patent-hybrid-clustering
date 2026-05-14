--get citation counts for patents
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
)

SELECT DISTINCT
  cluster_id,
  id AS family_id,
  Ncit AS citations
FROM
  cit_clust
WHERE
  Ncit IS NOT NULL
