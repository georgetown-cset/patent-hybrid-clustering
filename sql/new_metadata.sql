WITH get_original_set AS (
  SELECT DISTINCT
    metadata.*
  FROM
    unified_patents.metadata
  LEFT JOIN
    staging_patent_clusters.cluster_assignment_staging
    USING
      (family_id)
  WHERE
    NOT((REGEXP_CONTAINS(SPLIT(patent_id, '-')[OFFSET(2)], 'P')
      OR REGEXP_CONTAINS(SPLIT(patent_id, '-')[OFFSET(2)], 'S'))
      AND SPLIT(patent_id, '-')[OFFSET(0)] = 'US')
    AND (cluster_assignment_staging.family_id IS NULL)
),

test_for_text AS (
  SELECT
    CASE WHEN COALESCE(title, title_original, abstract, abstract_original) IS NULL THEN 0
      ELSE 1 END AS has_text,
    patent_id,
    family_id
  FROM
    get_original_set
),

test_for_family_text AS (
  SELECT
    SUM(has_text) AS null_check,
    family_id
  FROM
    test_for_text
  GROUP BY
    family_id
)

SELECT DISTINCT
  get_original_set.*
FROM
  get_original_set
LEFT JOIN
  test_for_family_text
  USING
    (family_id)
WHERE null_check > 0
