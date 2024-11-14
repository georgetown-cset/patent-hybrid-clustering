-- Mean-scaling the weights
WITH sums AS (
  SELECT
    avg(weight_citation) AS mean_citation,
    avg(weight_text) AS mean_text,
    avg(weight_cpc) AS mean_cpc
  FROM staging_patent_clusters.combined_weights
)

SELECT
  family_id AS id,
  family_reference AS ref_id,
  (weight_citation / mean_citation + weight_text / mean_text + weight_cpc / mean_cpc) / 3 AS weight
FROM staging_patent_clusters.combined_weights, sums
