SELECT
  text_match.family_id,
  similar.family_id AS family_reference,
  similar.similarity as weight
 FROM
  gcp-cset-projects.tmp.cpc_embeddings_HNSW_out_20240720 as text_match
CROSS JOIN
  UNNEST(most_similar) AS similar
WHERE
  similar.family_id != text_match.family_id