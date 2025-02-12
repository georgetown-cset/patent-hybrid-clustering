SELECT
  new_most_similar_cpc.family_id,
  similar.family_id AS family_reference,
  similar.similarity AS weight
FROM
  staging_patent_clusters.new_most_similar_cpc
CROSS JOIN
  UNNEST(most_similar) AS similar
WHERE
  similar.family_id != new_most_similar_cpc.family_id
