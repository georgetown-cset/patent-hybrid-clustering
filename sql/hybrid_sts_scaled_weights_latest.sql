-- This version may still include dummy families that no longer exist
-- We choose not to remove these here because we also want to retain the weights for family ids
-- that may not yet have been connected to the map (because their only link was to a reference also
-- not yet in the map) and may get connected in the future.
-- This means if you use this table for active work you should INNER JOIN it to the assignment table
SELECT DISTINCT
  id,
  ref_id,
  weight
FROM
  staging_patent_clusters.hybrid_sts_scaled_weights_latest
UNION DISTINCT
SELECT DISTINCT
  id,
  ref_id,
  weight
FROM
  staging_patent_clusters.new_hybrid_sts_scaled_weights
