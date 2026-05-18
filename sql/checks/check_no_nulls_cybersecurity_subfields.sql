-- Check that cybersecurity_subfields fields that shouldn't be null aren't
SELECT LOGICAL_AND(cluster_id IS NOT NULL)
  AND LOGICAL_AND(cybersecurity_fields_agg IS NOT NULL)
FROM
  staging_patent_clusters.cybersecurity_subfields
