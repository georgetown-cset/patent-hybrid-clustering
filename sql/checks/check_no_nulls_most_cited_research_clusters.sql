-- Keywords can be null if a research cluster has no keywords
SELECT LOGICAL_AND(research_cluster_id IS NOT NULL)
  AND LOGICAL_AND(cluster_id IS NOT NULL)
  AND LOGICAL_AND(citations IS NOT NULL)
  AND LOGICAL_AND(citation_rank IS NOT NULL)
FROM
  staging_patent_clusters.most_cited_research_clusters
