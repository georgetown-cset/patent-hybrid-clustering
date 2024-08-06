SELECT
  text_match.family_id,
  similar.family_id AS family_reference,
  similar.similarity as weight
FROM
  tmp.full_title_abstract_embeddings_HNSW_cos_sim_20240707 AS text_match
CROSS JOIN
  UNNEST(most_similar) AS similar
WHERE
  similar.family_id != text_match.family_id