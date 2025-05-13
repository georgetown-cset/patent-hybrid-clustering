-- Combine previous translated patents with translated patent updates to get
-- current translated patents in the active clustering

SELECT DISTINCT
  *
FROM
  staging_patent_clusters.translated_patents
UNION DISTINCT
SELECT DISTINCT
  *
FROM
  staging_patent_clusters.new_translated_patents
