SELECT DISTINCT
  references.family_id,
  links.family_id AS family_reference
FROM
  unified_patents.references
LEFT JOIN
  unified_patents.links
  ON
    references.patent_reference = links.patent_id
WHERE
  references.family_id != links.family_id
