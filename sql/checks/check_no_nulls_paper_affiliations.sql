-- Check that paper_affiliations fields that shouldn't be null aren't
SELECT LOGICAL_AND(cluster_id IS NOT NULL)
  AND LOGICAL_AND(org_name IS NOT NULL)
  AND LOGICAL_AND(affiliation_rank IS NOT NULL)
  AND LOGICAL_AND(NPF_ref_affiliation IS NOT NULL)
FROM
  staging_patent_clusters.paper_affiliations
