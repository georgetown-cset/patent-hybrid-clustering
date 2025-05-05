-- Join old most_similar_text table with updated most_similar_text table for current version

WITH all_ids AS (
  SELECT DISTINCT family_id
  FROM
    staging_patent_clusters.most_similar_text
  UNION DISTINCT
  SELECT DISTINCT family_id
  FROM
    staging_patent_clusters.new_most_similar_text
)

SELECT
  COALESCE(most_similar_text.most_similar, new_most_similar_text.most_similar) AS most_similar,
  family_id
FROM
  all_ids
LEFT JOIN
  staging_patent_clusters.most_similar_text
  USING
    (family_id)
LEFT JOIN
  staging_patent_clusters.new_most_similar_text
  USING
    (family_id)
