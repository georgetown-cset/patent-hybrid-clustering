SELECT
  tmp.cpc_embeddings_HNSW_out_20240720.family_id,
  similar.family_id AS family_reference,
  similar.similarity AS weight
FROM
  tmp.cpc_embeddings_HNSW_out_20240720
CROSS JOIN
  UNNEST(most_similar) AS similar
WHERE
  similar.family_id != tmp.cpc_embeddings_HNSW_out_20240720.family_id
