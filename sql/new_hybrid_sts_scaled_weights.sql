-- Scale the new weights for the patents being added to the clustering
-- Since the clustering has already been done, and we're just adding in new patents,
-- we have a hard-coded in alpha and beta; we don't want to try to adjust these
-- as we want the alpha and beta to be the same for every patent in the clustering
-- and we don't want to adjust the weights for the patents that have already been clustered
WITH get_alpha_beta AS (
  SELECT
    0.91078609897767326 AS alpha,
    0.046888892827964893 AS beta
  FROM unnest( [1] )
)

SELECT
  family_id AS id,
  family_reference AS ref_id,
  (alpha + ((1 - alpha - beta) / 4)) * weight_citation
  + (beta + ((1 - alpha - beta) / 4)) * weight_text
  + ((1 - alpha - beta) / 2) * weight_cpc AS weight
FROM staging_patent_clusters.new_combined_weights, get_alpha_beta
