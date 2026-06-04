-- Staging table for identifying the top three patent very broad categories for each cluster

-- Get dummy families
WITH
families_with_dummies AS (
  SELECT
    patent_id,
    COALESCE(family_id,
      "X-" || patent_id) AS family_id
  FROM
    unified_patents.links
),

-- Get patent clusters, with all patent ids in the families
clusters AS (
  SELECT DISTINCT
    patent_id,
    family_id,
    cluster_assignment.cluster_id
  FROM
    staging_patent_clusters.cluster_assignment
  LEFT JOIN
    families_with_dummies
    USING
      (family_id)
),

/* category prediction */
category_tab AS (
  SELECT
    family_id,
    very_broad_category
  FROM
    patent_categories.family_categories
),

/* Merge clusters and category predictions */
merged AS (
  SELECT DISTINCT
    clusters.family_id,
    cluster_id,
    very_broad_category
  FROM
    clusters
  LEFT JOIN
    category_tab
    USING
      (family_id)
),

percentages AS (
  SELECT
    cluster_id,
    SUM(IF(very_broad_category = "Food and Agriculture", 1, 0)) / NULLIF(COUNT(family_id), 0) AS food_and_agriculture,
    SUM(
      IF(very_broad_category = "Chemicals and Waste Treatment", 1, 0)
    ) / NULLIF(COUNT(family_id), 0) AS chemicals_and_waste_treatment,
    SUM(
      IF(very_broad_category = "Services (Business, Education, Entertainment)", 1, 0)
    ) / NULLIF(COUNT(family_id), 0) AS services,
    SUM(
      IF(very_broad_category = "Information and Communications Technology", 1, 0)
    ) / NULLIF(COUNT(family_id), 0) AS information_and_communications_technology,
    SUM(
      IF(very_broad_category = "Manufacturing, Construction and Transportation", 1, 0)
    ) / NULLIF(COUNT(family_id), 0) AS manufacturing_construction_transportation,
    SUM(IF(very_broad_category = "Energy", 1, 0)) / NULLIF(COUNT(family_id), 0) AS energy,
    SUM(IF(very_broad_category = "Life Sciences", 1, 0)) / NULLIF(COUNT(family_id), 0) AS life_sciences,
    SUM(
      IF(very_broad_category = "Scientific and Technical Equipment", 1, 0)
    ) / NULLIF(COUNT(family_id), 0) AS scientific_and_technical_equipment
  FROM
    merged
  GROUP BY
    cluster_id
),

unpivoted AS (
  SELECT
    *
  FROM
    percentages UNPIVOT(percentage FOR very_broad_category IN (
      food_and_agriculture,
      chemicals_and_waste_treatment,
      services,
      information_and_communications_technology,
      manufacturing_construction_transportation,
      energy,
      life_sciences,
      scientific_and_technical_equipment))
),

ordered_percentages AS (
  SELECT
    cluster_id,
    very_broad_category,
    percentage,
    ROW_NUMBER() OVER (PARTITION BY cluster_id ORDER BY percentage DESC) AS cat_rank
  FROM
    unpivoted
),

category_cleanup AS (
  SELECT DISTINCT
    cluster_id,
    CASE WHEN very_broad_category = "food_and_agriculture" THEN "Food and Agriculture"
      WHEN very_broad_category = "chemicals_and_waste_treatment" THEN "Chemicals and Waste Treatment"
      WHEN very_broad_category = "services" THEN "Services (Business, Education, Entertainment)"
      WHEN very_broad_category = "information_and_communications_technology"
        THEN "Information and Communications Technology"
      WHEN very_broad_category = "manufacturing_construction_transportation"
        THEN "Manufacturing, Construction and Transportation"
      WHEN very_broad_category = "energy" THEN "Energy"
      WHEN very_broad_category = "life_sciences" THEN "Life Sciences"
      WHEN very_broad_category = "scientific_and_technical_equipment" THEN "Scientific and Technical Equipment"
    END AS very_broad_category,
    percentage,
    cat_rank
  FROM
    ordered_percentages
)

SELECT
  cluster_id,
  IF(cat_rank = 1, very_broad_category, NULL) AS class_cat,
  very_broad_category,
  percentage,
  cat_rank AS category_rank
FROM
  category_cleanup
WHERE
  cat_rank <= 3
ORDER BY
  cluster_id
