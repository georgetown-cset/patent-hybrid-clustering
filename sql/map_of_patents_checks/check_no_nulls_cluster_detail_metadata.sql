-- Check that paper_affiliations fields that shouldn't be null aren't
SELECT LOGICAL_AND(cluster_id IS NOT NULL)
FROM
  staging_map_of_patents.cluster_detail_metadata
