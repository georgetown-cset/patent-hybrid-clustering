SELECT
  {table_for_cpc_faiss_results}.family_id,
  similar.family_id AS family_reference,
  similar.similarity AS weight
FROM
  staging_patent_clusters.{table_for_cpc_faiss_results}
CROSS JOIN
  UNNEST(most_similar) AS similar
WHERE
  similar.family_id != {table_for_cpc_faiss_results}.family_id
