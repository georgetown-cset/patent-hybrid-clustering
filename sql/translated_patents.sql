SELECT DISTINCT
  *
FROM
  staging_patent_clusters.translated_patents
UNION DISTINCT
SELECT DISTINCT
  *
FROM
  staging_patent_clusters.new_translated_patents
