WITH all_ids AS (
  SELECT DISTINCT family_id
  FROM
    staging_patent_clusters.most_similar_cpc
  UNION DISTINCT
  SELECT DISTINCT family_id
  FROM
    staging_patent_clusters.new_most_similar_cpc
)

SELECT
  COALESCE(most_similar_cpc.most_similar, new_most_similar_cpc.most_similar) AS most_similar,
  family_id
FROM
  all_ids
LEFT JOIN
  staging_patent_clusters.most_similar_cpc
  USING
    (family_id)
LEFT JOIN
  staging_patent_clusters.new_most_similar_cpc
  USING
    (family_id)
