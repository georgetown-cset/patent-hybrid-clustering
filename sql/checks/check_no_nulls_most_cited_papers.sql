SELECT LOGICAL_AND(merged_id IS NOT NULL)
  AND LOGICAL_AND(cluster_id IS NOT NULL)
  AND LOGICAL_AND(citations IS NOT NULL)
  AND LOGICAL_AND(citation_rank IS NOT NULL)
  AND LOGICAL_AND(most_cited_title IS NOT NULL)
FROM
  staging_patent_clusters.most_cited_papers
