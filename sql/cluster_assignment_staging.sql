-- Take the current version of the cluster assignment and update it to
-- account for new patent family assignments; that is, since the last run
-- some patents that were already clustered may have been assigned to patent
-- families and we need to place them in those families and adjust clusters

-- get the dummy families we potentially want to replace
WITH
get_dummy_families AS (
  SELECT DISTINCT
    family_id,
    cluster_id,
    SUBSTRING(family_id, 3) AS patent_id
  FROM
    staging_patent_clusters.cluster_assignment
  WHERE
    REGEXP_CONTAINS(family_id, r'^X')
),

-- find the new families that we could replace them with
dummy_replacement_families AS (
  SELECT DISTINCT
    metadata.family_id,
    cluster_id,
    get_dummy_families.family_id AS dummy_family_to_remove
  FROM
    get_dummy_families
  INNER JOIN
    unified_patents.metadata
    USING (patent_id)
  WHERE
    metadata.family_id IS NOT NULL
),

-- count how many times each potential replacement family would appear in our clusters
-- this is basically dealing with the fact that our different dummy families may have
-- been assigned to different clusters
replacements_counting AS (
  SELECT
    family_id,
    COUNT(DISTINCT cluster_id) AS cluster_count
  FROM
    dummy_replacement_families
  GROUP BY
    family_id
),

-- find the ones that were assigned to multiple clusters
replacements_in_multiple_clusters AS (
  SELECT DISTINCT family_id
  FROM
    replacements_counting
  WHERE
    cluster_count > 1
),

-- link those ones back to their original patents, and to their dummy family ids
original_patents AS (
  SELECT DISTINCT
    family_id,
    dummy_family_to_remove,
    SUBSTRING(dummy_family_to_remove, 3) AS patent_id
  FROM
    replacements_in_multiple_clusters
  LEFT JOIN
    dummy_replacement_families
    USING
      (family_id)
),

-- get initial date consolidated
date_data AS (
  SELECT DISTINCT
    original_patents.family_id,
    patent_id,
    COALESCE(first_priority_date, application_date, publication_date) AS patent_date
  FROM
    original_patents
  INNER JOIN
    unified_patents.dates
    USING
      (patent_id)
),

-- Get title and date info
title_date_info AS (
  SELECT DISTINCT
    date_data.family_id,
    patent_id,
    COALESCE(title, title_original) AS title,
    COALESCE(abstract, abstract_original) AS abstract,
    patent_date
  FROM
    unified_patents.metadata
  INNER JOIN
    date_data
    USING
      (patent_id)
),

-- get repeated patents in an order
get_ordering AS (
  SELECT DISTINCT
    family_id,
    patent_id,
    RANK() OVER (
      PARTITION BY family_id ORDER BY patent_date ASC, LENGTH(title), LENGTH(abstract), patent_id DESC
    ) AS ordering
  FROM
    title_date_info
  ORDER BY family_id, ordering
),

-- Select just one entry from that order
get_ordered AS (
  SELECT DISTINCT
    get_ordering.family_id,
    patent_id,
    dummy_family_to_remove
  FROM
    get_ordering
  LEFT JOIN
    original_patents
    USING
      (patent_id)
  WHERE ordering = 1
),

-- link our chosen single entry back to the clusters
link_to_clusters AS (
  SELECT
    cluster_id,
    get_ordered.family_id
  FROM
    get_ordered
  INNER JOIN
    staging_patent_clusters.cluster_assignment_archived
    ON
      dummy_family_to_remove = cluster_assignment_archived.family_id
),

-- find non-duplicated dummies
-- basically we just handled all the dummy replacements that are in multiple clusters but most are in just one
-- deal with those now
non_duplicated AS (
  SELECT DISTINCT
    dummy_replacement_families.cluster_id,
    family_id
  FROM
    dummy_replacement_families
  LEFT JOIN
    link_to_clusters
    USING
      (family_id)
  WHERE
    link_to_clusters.family_id IS NULL
),

original_assignments_minus_replacements AS (
  SELECT DISTINCT
    cluster_assignment.cluster_id,
    cluster_assignment.family_id
  FROM
    staging_patent_clusters.cluster_assignment
  LEFT JOIN
    non_duplicated
    USING
      (family_id)
  LEFT JOIN
    link_to_clusters
    USING
      (family_id)
  WHERE non_duplicated.family_id IS NULL AND link_to_clusters.family_id IS NULL
)

SELECT DISTINCT
  *
FROM
  original_assignments_minus_replacements
UNION DISTINCT
SELECT DISTINCT
  cluster_id,
  family_id
FROM
  non_duplicated
UNION DISTINCT
SELECT DISTINCT
  cluster_id,
  family_id
FROM
  link_to_clusters
