with sums as (
  select
    sum(weight_citation) as sum_DC,
    sum(weight_text) as sum_ST,
    sum(weight_cpc) as sum_SC
  from staging_patent_clusters.combined_weights
),

get_alpha_beta as (
  select
    1 / (sum_DC / sum_SC + 1 + sum_DC / sum_ST) as alpha,
    (1 / (sum_DC / sum_SC + 1 + sum_DC / sum_ST)) * sum_DC / sum_ST as beta
  from sums
)

select
  family_id as id,
  family_reference as ref_id,
  alpha * weight_citation + beta * weight_text + (1 - alpha - beta) * weight_cpc as weight
from staging_patent_clusters.combined_weights, get_alpha_beta