-- Check that priority_country fields that shouldn't be null aren't
SELECT LOGICAL_AND(cluster_id IS NOT NULL)
  AND LOGICAL_AND(country IS NOT NULL)
  AND LOGICAL_AND(country_rank IS NOT NULL)
  AND LOGICAL_AND(NPF_country IS NOT NULL)
  AND LOGICAL_AND(NPF_top10_countries IS NOT NULL)
  AND LOGICAL_AND(NPF_missing_all_countries IS NOT NULL)
FROM
  staging_patent_clusters.priority_country
