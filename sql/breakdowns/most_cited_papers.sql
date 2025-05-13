-- Find most cited research publications by cluster

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

-- Linking patents to their referenced papers
pat_paper_link AS (
  SELECT
    patent_id,
    merged_id AS ref_id
  FROM
    unified_patents.publication_references
),

-- Linking all papers that are cited by any patent in
-- a patent family in the cluster to the cluster
clust_cit_link AS (
  SELECT
    cluster_id,
    ref_id
  FROM
    clusters
  LEFT JOIN
    pat_paper_link
    USING
      (patent_id)
),

new_art AS (
  -- assume 150 DAYS lag between expected publication and expected import date
  SELECT merged_id
  FROM
    map_of_science.cluster_assignment
  INNER JOIN
    literature.papers
    USING
      (merged_id)
  WHERE
    DATE_DIFF(publication_date, DATE_SUB(CURRENT_DATE(), INTERVAL 1975 DAY), DAY) > 0
),

new_pat AS (
  -- assume 150 DAYS lag between expected publication and expected import date
  -- for patents we use ten years instead of 5 for papers
  SELECT
    *
  FROM
    clusters
  INNER JOIN (
    SELECT
      patent_id,
      family_id,
      first_priority_date,
      EXTRACT(YEAR
        FROM
        CURRENT_DATE()) - EXTRACT(YEAR
        FROM
        first_priority_date) AS age
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
    merged_id AS publication_reference
  FROM
    clusters
  INNER JOIN
    new_pat
    USING (patent_id)
  LEFT JOIN
    unified_patents.publication_references
    USING
      (patent_id)
),

-- get new article citations of new patents
new_cit AS (
  SELECT
    COUNT(DISTINCT cit.family_id) AS Ncit,
    merged_id AS id
  FROM
    cit
  INNER JOIN
    unified_patents.publication_references
    USING
      (patent_id)
  WHERE
    merged_id IN (
      SELECT merged_id
      FROM
        new_art
    )
  GROUP BY
    merged_id
),

-- link citations to clusters
cit_clust AS (
  SELECT
    *
  FROM
    clust_cit_link
  INNER JOIN
    new_cit
    ON
      clust_cit_link.ref_id = new_cit.id
),

-- prepare titles for cit
cit_tit_corp AS (
  SELECT
    merged_id,
    CONCAT(
      IF(title IS NULL, '', title), ', ', year, ', ', source_title) AS most_cited_title
  FROM (
    SELECT
      merged_id,
      IF(title_english IS NULL, title_foreign, title_english) AS title,
      IF(year IS NULL, '', CAST(year AS STRING)) AS year,
      IF(source_title IS NULL, '', source_title ) AS source_title
    FROM
      literature.papers
  )
),

-- get cited papers
cit_rank AS (
  SELECT DISTINCT
    cluster_id,
    id AS merged_id,
    Ncit AS citations
  FROM
    cit_clust
  WHERE
    Ncit IS NOT NULL
),

cit_title AS (
  SELECT DISTINCT
    *
  FROM
    cit_rank
  LEFT JOIN
    cit_tit_corp
    USING
      (merged_id)
)

SELECT DISTINCT
  * EXCEPT(most_cited_title),
  ROW_NUMBER() OVER (PARTITION BY cluster_id ORDER BY citations DESC) AS citation_rank,
  most_cited_title
FROM
  cit_title
ORDER BY
  cluster_id,
  citation_rank
