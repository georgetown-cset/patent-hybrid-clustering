-- Staging table for identifying the top three patent broad categories for each cluster

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
    broad_category
  FROM
    patent_categories.family_categories
),

/* Merge clusters and category predictions */
merged AS (
  SELECT DISTINCT
    clusters.family_id,
    cluster_id,
    broad_category
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
    SUM(IF(broad_category = "Food and Agriculture", 1, 0)) / NULLIF(COUNT(family_id), 0) AS food_and_agriculture,
    SUM(
      IF(broad_category = "Chemicals and Waste Treatment", 1, 0)
    ) / NULLIF(COUNT(family_id), 0) AS chemicals_and_waste_treatment,
    SUM(
      IF(broad_category = "Services (Business, Education, Entertainment)", 1, 0)
    ) / NULLIF(COUNT(family_id), 0) AS services,
    SUM(IF(broad_category = "Computing", 1, 0)) / NULLIF(COUNT(family_id), 0) AS computing,
    SUM(IF(broad_category = "Construction and Mining", 1, 0)) / NULLIF(COUNT(family_id), 0) AS construction_and_mining,
    SUM(IF(broad_category = "Energy", 1, 0)) / NULLIF(COUNT(family_id), 0) AS energy,
    SUM(IF(broad_category = "Life Sciences", 1, 0)) / NULLIF(COUNT(family_id), 0) AS life_sciences,
    SUM(IF(broad_category = "Manufacturing", 1, 0)) / NULLIF(COUNT(family_id), 0) AS manufacturing,
    SUM(
      IF(broad_category = "Scientific and Technical Equipment", 1, 0)
    ) / NULLIF(COUNT(family_id), 0) AS scientific_and_technical_equipment,
    SUM(
      IF(broad_category = "Semiconductors, Electronics and Nanotechnology", 1, 0)
    ) / NULLIF(COUNT(family_id), 0) AS semiconductors_electronics_nanotechnology,
    SUM(IF(broad_category = "Telecommunications", 1, 0)) / NULLIF(COUNT(family_id), 0) AS telecommunications,
    SUM(IF(broad_category = "Transportation", 1, 0)) / NULLIF(COUNT(family_id), 0) AS transportation
  FROM
    merged
  GROUP BY
    cluster_id
),

unpivoted AS (
  SELECT
    *
  FROM
    percentages UNPIVOT(percentage FOR broad_category IN (
      food_and_agriculture,
      chemicals_and_waste_treatment,
      services,
      computing,
      construction_and_mining,
      energy,
      life_sciences,
      manufacturing,
      scientific_and_technical_equipment,
      semiconductors_electronics_nanotechnology,
      telecommunications,
      transportation))
),

ordered_percentages AS (
  SELECT
    cluster_id,
    broad_category,
    percentage,
    ROW_NUMBER() OVER (PARTITION BY cluster_id ORDER BY percentage DESC) AS cat_rank
  FROM
    unpivoted
),

category_cleanup AS (
  SELECT DISTINCT
    cluster_id,
    CASE WHEN broad_category = "food_and_agriculture" THEN "Food and Agriculture"
      WHEN broad_category = "chemicals_and_waste_treatment" THEN "Chemicals and Waste Treatment"
      WHEN broad_category = "services" THEN "Services (Business, Education, Entertainment)"
      WHEN broad_category = "computing" THEN "Computing"
      WHEN broad_category = "construction_and_mining" THEN "Construction and Mining"
      WHEN broad_category = "energy" THEN "Energy"
      WHEN broad_category = "life_sciences" THEN "Life Sciences"
      WHEN broad_category = "manufacturing" THEN "Manufacturing"
      WHEN broad_category = "scientific_and_technical_equipment" THEN "Scientific and Technical Equipment"
      WHEN broad_category = "semiconductors_electronics_nanotechnology"
        THEN "Semiconductors, Electronics and Nanotechnology"
      WHEN broad_category = "telecommunications" THEN "Telecommunications"
      WHEN broad_category = "transportation" THEN "Transportation"
    END AS broad_category,
    percentage,
    cat_rank
  FROM
    ordered_percentages
)

SELECT
  cluster_id,
  IF(cat_rank = 1, broad_category, NULL) AS class_cat,
  broad_category,
  percentage,
  cat_rank AS category_rank
FROM
  category_cleanup
WHERE
  cat_rank <= 3
ORDER BY
  cluster_id
