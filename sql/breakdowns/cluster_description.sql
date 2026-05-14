 /* get clusters */
WITH
families_with_dummies AS (
  SELECT
    patent_id,
    COALESCE(family_id,
      "X-" || patent_id) AS family_id
  FROM
    unified_patents.links
),

clusters AS (
  SELECT DISTINCT
    family_id,
    cluster_assignment.cluster_id
  FROM
    staging_patent_clusters.cluster_assignment
),

/* cluster-level age, growth and patent_vitality */
growth_tab AS (
  SELECT
    cluster_id,
    age,
    patent_vit,
    NPF_all_years,
    NPF_last_10_year,
    year_n,
    year_n_1,
    year_n_2,
    year_n_3,
    year_n_4,
    year_n_5,
    growth_1yr,
    growth_3yr,
    growth_5yr,
    share_growth_1yr,
    share_growth_3yr,
    share_growth_5yr,
    growth_1yr_p_rank,
    growth_3yr_p_rank,
    growth_5yr_p_rank,
    mean_growth_3_year_p_rank
  FROM
    staging_patent_clusters.growth
),

pri_country as (
  SELECT
    DISTINCT cluster_id,
    country AS top_country
  FROM
    staging_patent_clusters.priority_country
  WHERE
    country_rank = 1
    AND country IS NOT NULL
),

top_3_pri_countries as (
  SELECT
    cluster_id,
    ARRAY_AGG(country ORDER BY country_rank) AS top_3_country
  FROM
    staging_patent_clusters.priority_country
  WHERE
    country_rank <= 3
    AND country IS NOT NULL
  GROUP BY
    cluster_id
),

/* The country dominance */
country_dom_r AS ( /* null country is not included in ranking */
  SELECT
    cluster_id,
    top_country,
    top_3_country
    FROM pri_country
    LEFT JOIN top_3_pri_countries
    USING (cluster_id)
),

/* Get AI and other predictions */
predictions AS (
  SELECT
    DISTINCT cluster_id,
    pred_ai,
    nlp_pred,
    Computer_Vision_pred as cv_pred,
    pred_robotics as ro_pred,
    pred_cybersecurity as cyber_pred,
    pred_semiconductors as semiconductor_pred
  FROM
    staging_patent_clusters.ai_pred
  LEFT JOIN
    staging_patent_clusters.robotics_pred
    USING (cluster_id)
  LEFT JOIN
    staging_patent_clusters.cybersecurity_pred
    USING (cluster_id)
  LEFT JOIN
    staging_patent_clusters.semiconductors_pred
    USING (cluster_id)
),

/* add field color data */
field_color AS (
  SELECT
    cluster_id,
    class_cat,
    categories,
    categories_agg
  FROM
    staging_patent_clusters.patent_class
),

/* add assignee type shares */
ind_sh AS (
  SELECT
    cluster_id,
    Company_incl_miss AS industry_share,
    Education_incl_miss AS education_share,
    Nonprofit_incl_miss as nonprofit_share,
    Government_incl_miss as government_share
  FROM
    staging_patent_clusters.assignee_type
),

/* top five CSET-extracted phrases */
top_phrase AS (
SELECT
  cluster_id,
  STRING_AGG(cset_extracted_phrase, ', ') AS cset_extracted_phrase
FROM (
  SELECT
    cluster_id,
    cset_extracted_phrase,
    ROW_NUMBER() OVER (PARTITION BY cluster_id ORDER BY MIN(score) ASC) AS phrase_rank
  FROM
    staging_patent_clusters.phrases
  GROUP BY
    cluster_id,
    cset_extracted_phrase )
WHERE
  phrase_rank < 6
GROUP BY
  cluster_id
),

cluster_mod AS (
SELECT
  *
FROM
  staging_patent_clusters.modularity
)

SELECT
  cluster_id,
  class_cat,
  categories,
  categories_agg,
  top_country,
  top_3_country,
  pred_ai as ai_pred,
  nlp_pred,
  cv_pred,
  ro_pred,
  cyber_pred,
  semiconductor_pred,
  cset_extracted_phrase,
  age,
  patent_vit,
  NPF_all_years,
  NPF_last_10_year,
  year_n,
  year_n_1,
  year_n_2,
  year_n_3,
  year_n_4,
  year_n_5,
  growth_1yr,
  growth_3yr,
  growth_5yr,
  share_growth_1yr,
  share_growth_3yr,
  share_growth_5yr,
  growth_1yr_p_rank,
  growth_3yr_p_rank,
  growth_5yr_p_rank,
  mean_growth_3_year_p_rank,
  cluster_modularity,
  IF (industry_share IS NULL,0, industry_share) AS industry_share,
  IF (education_share IS NULL,0, education_share) AS education_share,
  IF (nonprofit_share IS NULL,0, nonprofit_share) AS nonprofit_share,
  IF (government_share IS NULL,0, government_share) AS government_share,
  staging_patent_clusters.mk_eto_link(cluster_id) as link
FROM
  clusters
LEFT JOIN
  field_color USING (cluster_id)
LEFT JOIN
  country_dom_r USING (cluster_id)
LEFT JOIN
  predictions USING (cluster_id)
LEFT JOIN
  top_phrase USING (cluster_id)
LEFT JOIN
  growth_tab USING (cluster_id)
LEFT JOIN
  cluster_mod USING (cluster_id)
LEFT JOIN
  ind_sh USING (cluster_id)
