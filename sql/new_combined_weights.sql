-- Combine together the three weight features for all the new patents being added

WITH get_clean_weights AS (
  SELECT
    family_id,
    family_reference,
    coalesce(new_citation_weights.weight, 0) AS weight_citation,
    coalesce(new_text_weights.weight, 0) AS weight_text,
    coalesce(new_cpc_weights.weight, 0) AS weight_cpc
  FROM staging_patent_clusters.new_cpc_weights
  FULL OUTER JOIN staging_patent_clusters.new_text_weights USING (family_id, family_reference)
  FULL OUTER JOIN staging_patent_clusters.new_citation_weights USING (family_id, family_reference)
  LEFT JOIN staging_patent_clusters.new_patents_to_embed USING (family_id)
  WHERE text IS NOT NULL
)

SELECT
  get_clean_weights.family_id,
  family_reference,
  weight_citation,
  weight_text,
  weight_cpc
FROM get_clean_weights
LEFT JOIN staging_patent_clusters.new_patents_to_embed ON family_reference = new_patents_to_embed.family_id
