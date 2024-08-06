SELECT
  family_id,
  family_reference,
  coalesce(citation_weights.weight, 0) AS weight_citation,
  coalesce(text_weights.weight, 0) AS weight_text,
  coalesce(cpc_weights.weight, 0) AS weight_cpc
FROM staging_patent_clusters.cpc_weights
FULL OUTER JOIN staging_patent_clusters.text_weights USING (family_id, family_reference)
FULL OUTER JOIN staging_patent_clusters.citation_weights USING (family_id, family_reference)
