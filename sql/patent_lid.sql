SELECT DISTINCT
  patent_id,
  family_id,
  CAST(reliable AS BOOL) AS reliable,
  language_long,
  patent_lid.language
FROM
  staging_patent_clusters.patent_lid
LEFT JOIN
  unified_patents.metadata
  USING
    (patent_id)
UNION DISTINCT
SELECT DISTINCT
  *
FROM
  staging_patent_clusters.new_patent_lid
