/* Calculate the number of unique papers cited by patents from the patent clusters and their associated affils */
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
    merged_id
  FROM
    unified_patents.publication_references
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

-- Linking all papers that are cited by any patent in
-- a patent family in the cluster to the cluster
-- Limit to just the past 10 years of patents and past 5 years of papers
clust_cit_link AS (
  SELECT
    clusters.cluster_id,
    clusters.family_id,
    merged_id
  FROM
    clusters
  INNER JOIN
    new_pat
    USING (patent_id)
  LEFT JOIN
    pat_paper_link
    USING
      (patent_id)
  INNER JOIN
    new_art
    USING (merged_id)
),

/* add affiliates */
add_affiliations AS (
  SELECT
    cluster_id,
    org_name,
    ROW_NUMBER() OVER (PARTITION BY cluster_id ORDER BY COUNT(DISTINCT family_id) DESC) AS affiliation_rank,
    COUNT(DISTINCT family_id) AS NPF_ref_affiliation
  FROM
    clust_cit_link
  LEFT JOIN
    literature.affiliations
    USING
      (merged_id)
  WHERE
    org_name IS NOT NULL
  GROUP BY
    cluster_id,
    org_name
)

SELECT
  *
FROM
  add_affiliations
WHERE
  affiliation_rank < 10
ORDER BY
  cluster_id,
  affiliation_rank
