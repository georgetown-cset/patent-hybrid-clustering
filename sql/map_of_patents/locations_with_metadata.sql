WITH
phrases AS (
SELECT DISTINCT
  cluster_id,
  LOWER(TRIM(phrase)) as name,
FROM
  staging_patent_clusters.cluster_description
CROSS JOIN
  UNNEST(SPLIT(cset_extracted_phrase, ", ")) as phrase
),

broad_cat AS (
SELECT
  cluster_id,
  class_cat as name
FROM
  staging_patent_clusters.patent_class
),

cat AS (
  SELECT
    cluster_id,
    category.name as name,
    ROW_NUMBER() OVER(PARTITION BY cluster_id ORDER BY category.NPF DESC) AS ranking
  FROM
    staging_patent_clusters.patent_class
  CROSS JOIN
    UNNEST(categories) AS category
),

ai AS (
  SELECT
    cluster_id,
    field.name as name,
    ROW_NUMBER() OVER(PARTITION BY cluster_id ORDER BY field.percentage DESC) AS ranking
  FROM
    staging_patent_clusters.ai_subfields
  CROSS JOIN
    UNNEST(ai_fields) AS field
),

robotics AS (
  SELECT
    cluster_id,
    field.name as name,
    ROW_NUMBER() OVER(PARTITION BY cluster_id ORDER BY field.percentage DESC) AS ranking
  FROM
    staging_patent_clusters.robotics_subfields
  CROSS JOIN
    UNNEST(robotics_fields) AS field
),

biotech AS (
  SELECT
    cluster_id,
    field.name as name,
    ROW_NUMBER() OVER(PARTITION BY cluster_id ORDER BY field.percentage DESC) AS ranking
  FROM
    staging_patent_clusters.biotech_subfields
  CROSS JOIN
    UNNEST(biotech_fields) AS field
),

cybersecurity AS (
  SELECT
    cluster_id,
    field.name as name,
    ROW_NUMBER() OVER(PARTITION BY cluster_id ORDER BY field.percentage DESC) AS ranking
  FROM
    staging_patent_clusters.cybersecurity_subfields
  CROSS JOIN
    UNNEST(cybersecurity_fields) AS field
),

search_tags_pre_concept AS (
  SELECT
    cluster_id,
    STRUCT(name, "broad_category" as category) as tag
  FROM
    broad_cat
  UNION ALL
  SELECT
    cluster_id,
    STRUCT(name, "category" as category) as tag
  FROM
    cat
  WHERE
    ranking <= 3
  UNION ALL
  SELECT
    cluster_id,
    STRUCT(name, "ai_subfield" as category) as tag
  FROM
    ai
  UNION ALL
  SELECT
    cluster_id,
    STRUCT(name, "robotics_subfield" as category) as tag
  FROM
    robotics
  UNION ALL
  SELECT
    cluster_id,
    STRUCT(name, "biotech_subfield" as category) as tag
  FROM
    biotech
  UNION ALL
  SELECT
    cluster_id,
    STRUCT(name, "cybersecurity_subfield" as category) as tag
  FROM
    cybersecurity
  UNION ALL
  SELECT
    cluster_id,
    STRUCT("Artificial Intelligence" as name, "field" as category) as tag
  FROM
    staging_patent_clusters.ai_pred
  WHERE
    pred_ai >= 0.1
  UNION ALL
  SELECT
    cluster_id,
    STRUCT("Robotics" as name, "field" as category) as tag
  FROM
    staging_patent_clusters.robotics_pred
  WHERE
    pred_robotics >= 0.1
  UNION ALL
  SELECT
    cluster_id,
    STRUCT("Natural Language Processing" as name, "field" as category) as tag
  FROM
    staging_patent_clusters.ai_pred
  WHERE
    nlp_pred >= 0.1
  UNION ALL
  SELECT
    cluster_id,
    STRUCT("Cybersecurity" as name, "field" as category) as tag
  FROM
    staging_patent_clusters.cybersecurity_pred
  WHERE
    pred_cybersecurity >= 0.1
  UNION ALL
  SELECT
    cluster_id,
    STRUCT("Semiconductors" as name, "classifier" as category) as tag
  FROM
    staging_patent_clusters.semiconductors_pred
  WHERE
    pred_semiconductors >= 0.1
),

search_tags AS (
  SELECT
    cluster_id,
    tag
  FROM
    search_tags_pre_concept
  UNION ALL
  SELECT
    phrases.cluster_id,
    STRUCT(name, "phrase" as category) as tag
  FROM
    phrases
  LEFT JOIN
    search_tags_pre_concept
  ON
    LOWER(name) = LOWER(tag.name)
  WHERE
    tag.name IS NULL
),

search_tags_aggregated AS (
  SELECT
    cluster_id,
    ARRAY_AGG(tag) as tags
  FROM
    search_tags
  GROUP BY
    cluster_id
),

pubs AS (
  SELECT
    cluster_id,
    n_pubs,
    ROUND(100 * frac_citing_pubs, 2) AS frac_citing_pubs,
    ROUND(100 * n_pubs_percentile, 2) AS n_pubs_percentile
  FROM
    staging_patent_clusters.paper_count
),

meta AS (
  SELECT
    cluster_id,
    class_cat,
    categories,
    tags,
    top_3_country,
    NPF_all_years,
    NPF_last_10_year AS NPF,
    cset_extracted_phrase,
    cluster_title,
    cluster_summary,
    (
      SELECT STRING_AGG(name, ", ") FROM (SELECT name FROM UNNEST(categories) ORDER BY NPF DESC LIMIT 3)
    ) AS categories_agg,
    (
      SELECT STRING_AGG(name, ", ") FROM (SELECT name FROM UNNEST(ai_fields) ORDER BY percentage DESC)
    ) AS ai_fields_agg,
    (
      SELECT STRING_AGG(name, ", ") FROM (SELECT name FROM UNNEST(biotech_fields) ORDER BY percentage DESC)
    ) AS biotech_fields_agg,
    (
      SELECT STRING_AGG(name, ", ") FROM (SELECT name FROM UNNEST(cybersecurity_fields) ORDER BY percentage DESC)
    ) AS cybersecurity_fields_agg,
    (
      SELECT STRING_AGG(name, ", ") FROM (SELECT name FROM UNNEST(robotics_fields) ORDER BY percentage DESC)
    ) AS robotics_fields_agg,
    ROUND(100 * ai_pred, 2) AS ai_pred,
    ROUND(100 * cv_pred, 2) AS cv_pred,
    ROUND(100 * ro_pred, 2) AS ro_pred,
    ROUND(100 * nlp_pred, 2) AS nlp_pred,
    ROUND(100 * cyber_pred, 2) AS cyber_pred,
    ROUND(100 * semiconductor_pred, 2) AS semiconductor_pred,
    ROUND(age, 2) AS age,
    ROUND(100 * mean_growth_3_year_p_rank, 2) AS mean_growth_3_year_p_rank,
    ROUND(100 * cross_filing_percentile, 2) AS cross_filing_percentile,
    ROUND(100 * industry_share, 2) AS industry_share,
    ROUND(100 * education_share, 2) AS education_share,
    ROUND(100 * nonprofit_share, 2) AS nonprofit_share,
    ROUND(100 * government_share, 2) AS government_share,
    grant_percentage,
    grant_percentile
  FROM
    staging_patent_clusters.cluster_description
LEFT JOIN
  staging_patent_clusters.ai_subfields USING(cluster_id)
LEFT JOIN
  staging_patent_clusters.biotech_subfields USING(cluster_id)
LEFT JOIN
  staging_patent_clusters.cybersecurity_subfields USING(cluster_id)
LEFT JOIN
  staging_patent_clusters.robotics_subfields USING(cluster_id)
LEFT JOIN
  search_tags_aggregated USING(cluster_id)
LEFT JOIN
  staging_patent_clusters.cross_filing_percentage USING (cluster_id)
LEFT JOIN
  staging_patent_clusters.cluster_titles_summaries USING (cluster_id)
LEFT JOIN
  staging_patent_clusters.grant_rates USING (cluster_id)
)

SELECT DISTINCT
  meta.* EXCEPT(cluster_id),
  pubs.* EXCEPT(cluster_id),
  cluster_locations.cluster_id,
  cluster_locations.x,
  cluster_locations.y
FROM
  patent_clusters.cluster_locations
LEFT JOIN meta
  ON
    cluster_locations.cluster_id = meta.cluster_id
LEFT JOIN pubs
  ON
    pubs.cluster_id = meta.cluster_id
WHERE
  NPF IS NOT NULL AND NPF > 0 AND class_cat IS NOT NULL
