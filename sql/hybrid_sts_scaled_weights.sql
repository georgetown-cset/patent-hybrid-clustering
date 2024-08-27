-- Reweighting to halve the weight of CPCs and split the new weight between citations and text
WITH sums AS (
  SELECT
    sum(weight_citation) AS sum_DC,
    sum(weight_text) AS sum_ST,
    sum(weight_cpc) AS sum_SC
  FROM staging_patent_clusters.combined_weights
),

get_alpha_beta AS (
  SELECT
    1 / (sum_DC / sum_SC + 1 + sum_DC / sum_ST) AS alpha,
    (1 / (sum_DC / sum_SC + 1 + sum_DC / sum_ST)) * sum_DC / sum_ST AS beta
  FROM sums
)

SELECT
  family_id AS id,
  family_reference AS ref_id,
  (alpha + ((1 - alpha - beta) / 4)) * weight_citation
  + (beta + ((1 - alpha - beta) / 4)) * weight_text
  + ((1 - alpha - beta) / 2) * weight_cpc AS weight
FROM staging_patent_clusters.combined_weights, get_alpha_beta
