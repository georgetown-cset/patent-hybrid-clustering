-- Get initial map text weights

SELECT
  tmp.full_title_abstract_embeddings_HNSW_cos_sim_20240707.family_id,
  similar.family_id AS family_reference,
  similar.similarity AS weight
FROM
  tmp.full_title_abstract_embeddings_HNSW_cos_sim_20240707
CROSS JOIN
  UNNEST(most_similar) AS similar
WHERE
  similar.family_id != tmp.full_title_abstract_embeddings_HNSW_cos_sim_20240707.family_id
