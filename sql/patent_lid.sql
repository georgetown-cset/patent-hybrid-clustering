SELECT DISTINCT
  *
FROM
  staging_patent_clusters.patent_lid
UNION DISTINCT
SELECT DISTINCT
  *
FROM
  staging_patent_clusters.new_patent_lid