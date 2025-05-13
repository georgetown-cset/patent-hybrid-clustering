-- Staging table for identifying the top three patent categories for each cluster

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
    category
  FROM
    patent_categories.family_categories
),

/* Merge clusters and category predictions */
merged AS (
  SELECT DISTINCT
    clusters.family_id,
    cluster_id,
    category
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
    SUM(
      IF(category = "agriculture_forestry_fishing", 1, 0)
    ) / NULLIF(COUNT(family_id), 0) AS agriculture_forestry_fishing,
    SUM(
      IF(category = "wholesale_retail_motor_vehicle_repair", 1, 0)
    ) / NULLIF(COUNT(family_id), 0) AS wholesale_retail_motor_vehicle_repair,
    SUM(IF(category = "admin_and_support_services", 1, 0)) / NULLIF(COUNT(family_id), 0) AS admin_and_support_services,
    SUM(IF(category = "finance_and_insurance", 1, 0)) / NULLIF(COUNT(family_id), 0) AS finance_and_insurance,
    SUM(IF(category = "public_admin_and_defense", 1, 0)) / NULLIF(COUNT(family_id), 0) AS public_admin_and_defense,
    SUM(IF(category = "other_service_activities", 1, 0)) / NULLIF(COUNT(family_id), 0) AS other_service_activities,
    SUM(
      IF(category = "accommodation_and_food_service", 1, 0)
    ) / NULLIF(COUNT(family_id), 0) AS accommodation_and_food_service,
    SUM(IF(category = "real_estate", 1, 0)) / NULLIF(COUNT(family_id), 0) AS real_estate,
    SUM(
      IF(category = "professional_scientific_and_technical_activities", 1, 0)
    ) / NULLIF(COUNT(family_id), 0) AS professional_scientific_and_technical_activities,
    SUM(
      IF(category = "human_health_and_social_work_activities", 1, 0)
    ) / NULLIF(COUNT(family_id), 0) AS human_health_and_social_work_activities,
    SUM(
      IF(category = "information_and_communication", 1, 0)
    ) / NULLIF(COUNT(family_id), 0) AS information_and_communication,
    SUM(IF(category = "chemicals", 1, 0)) / NULLIF(COUNT(family_id), 0) AS chemicals,
    SUM(IF(category = "printing", 1, 0)) / NULLIF(COUNT(family_id), 0) AS printing,
    SUM(IF(category = "computing_equipment", 1, 0)) / NULLIF(COUNT(family_id), 0) AS computing_equipment,
    SUM(IF(category = "information_storage", 1, 0)) / NULLIF(COUNT(family_id), 0) AS information_storage,
    SUM(IF(category = "construction", 1, 0)) / NULLIF(COUNT(family_id), 0) AS construction,
    SUM(IF(category = "education", 1, 0)) / NULLIF(COUNT(family_id), 0) AS education,
    SUM(
      IF(category = "electrical_equipment_manufacturing", 1, 0)
    ) / NULLIF(COUNT(family_id), 0) AS electrical_equipment_manufacturing,
    SUM(
      IF(category = "electricity_gas_steam_ac_supply", 1, 0)
    ) / NULLIF(COUNT(family_id), 0) AS electricity_gas_steam_ac_supply,
    SUM(
      IF(category = "arts_entertainment_recreation", 1, 0)
    ) / NULLIF(COUNT(family_id), 0) AS arts_entertainment_recreation,
    SUM(IF(category = "food_and_tobacco", 1, 0)) / NULLIF(COUNT(family_id), 0) AS food_and_tobacco,
    SUM(IF(category = "biotechnology", 1, 0)) / NULLIF(COUNT(family_id), 0) AS biotechnology,
    SUM(
      IF(category = "diagnosis_medical_equipment", 1, 0)
    ) / NULLIF(COUNT(family_id), 0) AS diagnosis_medical_equipment,
    SUM(IF(category = "pharmaceuticals", 1, 0)) / NULLIF(COUNT(family_id), 0) AS pharmaceuticals,
    SUM(IF(category = "textile_manufacturing", 1, 0)) / NULLIF(COUNT(family_id), 0) AS textile_manufacturing,
    SUM(IF(category = "apparel_manufacturing", 1, 0)) / NULLIF(COUNT(family_id), 0) AS apparel_manufacturing,
    SUM(IF(category = "wood_manufacturing", 1, 0)) / NULLIF(COUNT(family_id), 0) AS wood_manufacturing,
    SUM(IF(category = "paper_manufacturing", 1, 0)) / NULLIF(COUNT(family_id), 0) AS paper_manufacturing,
    SUM(IF(category = "plastics_manufacturing", 1, 0)) / NULLIF(COUNT(family_id), 0) AS plastics_manufacturing,
    SUM(IF(category = "non_metals_manufacturing", 1, 0)) / NULLIF(COUNT(family_id), 0) AS non_metals_manufacturing,
    SUM(IF(category = "metals_manufacturing", 1, 0)) / NULLIF(COUNT(family_id), 0) AS metals_manufacturing,
    SUM(
      IF(category = "miscellaneous_manufacturing", 1, 0)
    ) / NULLIF(COUNT(family_id), 0) AS miscellaneous_manufacturing,
    SUM(
      IF(category = "layered_products_manufacturing", 1, 0)
    ) / NULLIF(COUNT(family_id), 0) AS layered_products_manufacturing,
    SUM(IF(category = "additive_manufacturing", 1, 0)) / NULLIF(COUNT(family_id), 0) AS additive_manufacturing,
    SUM(IF(category = "furniture_manufacturing", 1, 0)) / NULLIF(COUNT(family_id), 0) AS furniture_manufacturing,
    SUM(IF(category = "transportation_and_storage", 1, 0)) / NULLIF(COUNT(family_id), 0) AS transportation_and_storage,
    SUM(
      IF(category = "measuring_and_control_equipment", 1, 0)
    ) / NULLIF(COUNT(family_id), 0) AS measuring_and_control_equipment,
    SUM(IF(category = "weapons_manufacturing", 1, 0)) / NULLIF(COUNT(family_id), 0) AS weapons_manufacturing,
    SUM(IF(category = "mining_and_quarrying", 1, 0)) / NULLIF(COUNT(family_id), 0) AS mining_and_quarrying,
    SUM(IF(category = "nanotechnology", 1, 0)) / NULLIF(COUNT(family_id), 0) AS nanotechnology,
    SUM(IF(category = "optics_and_photography", 1, 0)) / NULLIF(COUNT(family_id), 0) AS optics_and_photography,
    SUM(
      IF(category = "semiconductors_and_electronics", 1, 0)
    ) / NULLIF(COUNT(family_id), 0) AS semiconductors_and_electronics,
    SUM(IF(category = "telecommunications", 1, 0)) / NULLIF(COUNT(family_id), 0) AS telecommunications,
    SUM(
      IF(category = "other_vehicle_transportation", 1, 0)
    ) / NULLIF(COUNT(family_id), 0) AS other_vehicle_transportation,
    SUM(
      IF(category = "road_vehicle_transportation", 1, 0)
    ) / NULLIF(COUNT(family_id), 0) AS road_vehicle_transportation,
    SUM(
      IF(category = "water_supply_sewage_waste_mgmt_remediation", 1, 0)
    ) / NULLIF(COUNT(family_id), 0) AS water_supply_sewage_waste_mgmt_remediation
  FROM
    merged
  GROUP BY
    cluster_id
),

unpivoted AS (
  SELECT
    *
  FROM
    percentages UNPIVOT(percentage FOR category IN (
      agriculture_forestry_fishing,
      wholesale_retail_motor_vehicle_repair,
      admin_and_support_services,
      finance_and_insurance,
      public_admin_and_defense,
      other_service_activities,
      accommodation_and_food_service,
      real_estate,
      professional_scientific_and_technical_activities,
      human_health_and_social_work_activities,
      information_and_communication,
      chemicals,
      printing,
      computing_equipment,
      information_storage,
      construction,
      education,
      electrical_equipment_manufacturing,
      electricity_gas_steam_ac_supply,
      arts_entertainment_recreation,
      food_and_tobacco,
      biotechnology,
      diagnosis_medical_equipment,
      pharmaceuticals,
      textile_manufacturing,
      apparel_manufacturing,
      wood_manufacturing,
      paper_manufacturing,
      plastics_manufacturing,
      non_metals_manufacturing,
      metals_manufacturing,
      miscellaneous_manufacturing,
      layered_products_manufacturing,
      additive_manufacturing,
      furniture_manufacturing,
      transportation_and_storage,
      measuring_and_control_equipment,
      weapons_manufacturing,
      mining_and_quarrying,
      nanotechnology,
      optics_and_photography,
      semiconductors_and_electronics,
      telecommunications,
      other_vehicle_transportation,
      road_vehicle_transportation,
      water_supply_sewage_waste_mgmt_remediation))
),

ordered_percentages AS (
  SELECT
    cluster_id,
    category,
    percentage,
    ROW_NUMBER() OVER (PARTITION BY cluster_id ORDER BY percentage DESC) AS cat_rank
  FROM
    unpivoted
)

SELECT
  cluster_id,
  IF(cat_rank = 1, category, NULL) AS class_cat,
  category,
  percentage,
  cat_rank AS category_rank
FROM
  ordered_percentages
WHERE
  cat_rank <= 3
ORDER BY
  cluster_id
