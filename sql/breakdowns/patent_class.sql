 /* aggregate class names and percentages into a string for presentation */
WITH
  ca_tab AS (
    SELECT
      cluster_id,
      class_cat
    FROM staging_patent_clusters.broad_category_staging
    WHERE class_cat IS NOT NULL
),

cluster_class_cat_ranks AS (
  SELECT
      cluster_id,
      class_cat,
      ROW_NUMBER() OVER(PARTITION BY cluster_id ORDER BY COUNT(*) DESC) AS class_cat_rank
  FROM ca_tab
  GROUP BY cluster_id, class_cat
),

patent_categories AS (
  SELECT
    cluster_id,
    category_name,
    category_name || ' (' || NPF || ')' AS text,
    NPF,
    category_rank
  FROM staging_patent_clusters.patent_category_staging
  ORDER BY cluster_id, category_rank
),

cluster_fields AS (
  SELECT
    cluster_id,
    ARRAY_AGG(STRUCT(category_name AS name, NPF) IGNORE NULLS ORDER BY category_rank) AS categories,
    STRING_AGG(text, ', ' ORDER BY category_rank) AS categories_agg,
  FROM patent_categories
  GROUP BY cluster_id
)

SELECT
    cluster_id,
    class_cat,
    categories,
    categories_agg
FROM cluster_class_cat_ranks
INNER JOIN cluster_fields USING(cluster_id)
