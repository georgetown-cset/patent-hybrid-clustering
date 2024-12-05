WITH dummy_references AS (
  SELECT DISTINCT
    patent_id,
    COALESCE(family_id, "X-" || patent_id) as family_id,
    patent_reference
  FROM
    unified_patents.references
),

dummy_links AS (
  SELECT DISTINCT
    patent_id,
    COALESCE(family_id, "X-" || patent_id) as family_id
  FROM
    unified_patents.links
)

SELECT DISTINCT
  dummy_references.family_id,
  links.family_id AS family_reference
FROM
  dummy_references
LEFT JOIN
  unified_patents.links
  ON
    dummy_references.patent_reference = links.patent_id
WHERE
  dummy_references.family_id != links.family_id