WITH dummy_references AS (
  SELECT DISTINCT
    patent_id,
    COALESCE(family_id, "X-" || patent_id) AS family_id,
    patent_reference
  FROM
    unified_patents.references
  INNER JOIN
    staging_patent_clusters.new_metadata
    USING (patent_id)
),

dummy_links AS (
  SELECT DISTINCT
    patent_id,
    COALESCE(family_id, "X-" || patent_id) AS family_id
  FROM
    unified_patents.links
)

SELECT DISTINCT
  dummy_references.family_id,
  dummy_links.family_id AS family_reference
FROM
  dummy_references
LEFT JOIN
  dummy_links
  ON
    dummy_references.patent_reference = dummy_links.patent_id
WHERE
  dummy_references.family_id != dummy_links.family_id
