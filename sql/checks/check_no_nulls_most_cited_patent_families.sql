-- Not every patent family has a title
SELECT LOGICAL_AND(family_id IS NOT NULL)
  AND LOGICAL_AND(cluster_id IS NOT NULL)
  AND LOGICAL_AND(citations IS NOT NULL)
  AND LOGICAL_AND(citation_rank IS NOT NULL)
FROM
  staging_patent_clusters.most_cited_patent_families
