-- check that each cluster id only appears once
SELECT COUNT(DISTINCT cluster_id) = COUNT(cluster_id)
FROM
  staging_map_of_patents.locations_with_metadata
