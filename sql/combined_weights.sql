SELECT
  family_id,
  family_reference,
  coalesce(citation_weights.weight, 0) as weight_citation,
  coalesce(text_weights.weight, 0) as weight_text,
  coalesce(cpc_weights.weight, 0) as weight_cpc
from staging_patent_clusters.cpc_weights
full outer join staging_patent_clusters.text_weights using (family_id, family_reference)
full outer join staging_patent_clusters.citation_weights using (family_id, family_reference)