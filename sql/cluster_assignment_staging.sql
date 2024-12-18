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

original_assignments_minus_replacements AS (
  SELECT DISTINCT
    cluster_assignment.cluster_id,
    cluster_assignment.family_id
  FROM
    staging_patent_clusters.cluster_assignment
  LEFT JOIN
    dummy_replacement_families
    ON
      (cluster_assignment.family_id = dummy_family_to_remove)
  WHERE dummy_family_to_remove IS NULL
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
  dummy_replacement_families
