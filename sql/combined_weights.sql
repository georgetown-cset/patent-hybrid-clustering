-- In this updated version of combined weights, we filter out any patents from the map with no text
WITH get_clean_weights AS (
  SELECT
    family_id,
    family_reference,
    coalesce(citation_weights.weight, 0) AS weight_citation,
    coalesce(text_weights.weight, 0) AS weight_text,
    coalesce(cpc_weights.weight, 0) AS weight_cpc
  FROM staging_patent_clusters.cpc_weights
  FULL OUTER JOIN staging_patent_clusters.text_weights USING (family_id, family_reference)
  FULL OUTER JOIN staging_patent_clusters.citation_weights USING (family_id, family_reference)
  LEFT JOIN staging_patent_clusters.patents_to_embed USING (family_id)
  WHERE text IS NOT NULL
)

SELECT
  get_clean_weights.family_id,
  family_reference,
  weight_citation,
  weight_text,
  weight_cpc
FROM get_clean_weights
LEFT JOIN staging_patent_clusters.patents_to_embed ON family_reference = patents_to_embed.family_id
WHERE text IS NOT NULL
