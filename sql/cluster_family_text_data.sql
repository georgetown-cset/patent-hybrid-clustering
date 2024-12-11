/*
Create table of cluster_id:family_id text data for downstream 
keyword extraction. 
*/
SELECT 
cluster_id, 
family_id, 
cpc_text, 
text AS title_abstract
FROM `staging_patent_clusters.cluster_assignment`
LEFT JOIN `staging_patent_clusters.family_cpc_text` USING(family_id)
LEFT JOIN `staging_patent_clusters.patents_to_embed` USING(family_id)