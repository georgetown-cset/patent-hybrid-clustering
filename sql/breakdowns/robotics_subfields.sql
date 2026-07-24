-- Get top robotics subfields for every cluster that's robotics-relevant
WITH
overall_filter AS (
  SELECT
    *
  FROM
    staging_patent_clusters.robotics_pred
  WHERE pred_robotics >= 0.1
),

unpivoted AS (
  SELECT
    *
  FROM
    overall_filter UNPIVOT(percentage FOR subfield IN (
      Robot_Type__Aerospace_pred,
      Robot_Type__Agriculture_pred,
      Robot_Type__Consumer_pred,
      Robot_Type__Delivery_pred,
      Robot_Type__Disaster_Response_pred,
      Robot_Type__Drones_pred,
      Robot_Type__Education_pred,
      Robot_Type__Entertainment_pred,
      Robot_Type__Humanoid_Exoskeleton_pred,
      Robot_Type__Industrial_pred,
      Robot_Type__Medical_pred,
      Robot_Type__Military_Security_pred,
      Robot_Type__Research_pred,
      Robot_Type__Service_pred,
      Robot_Type__Social_pred,
      Robot_Type__Telepresence_pred,
      Robot_Type__Transportation_pred,
      Robot_Type__Underwater_pred,
      Robot_Feature__Artificial_Intelligence_pred,
      Robot_Feature__Physical_Components_pred,
      Robot_Feature__Control_Systems_pred,
      Robot_Feature__Sensors_Measurement_pred,
      Robot_Feature__Speech_pred,
      Robot_Feature__Vision_pred
    ))
),

-- In addition to ranking here we also want to eliminate any fields that have 0 patents
-- We also want to change all our names to better names
ranked_fields AS (
  SELECT
    cluster_id,
    field_name AS subfield,
    percentage,
    ROW_NUMBER() OVER (PARTITION BY cluster_id ORDER BY percentage DESC) AS robotics_rank
  FROM unpivoted
  LEFT JOIN
    staging_patent_clusters.subfield_mappings
    USING (subfield)
  WHERE percentage > 0
),

-- We only want the top three fields or any field with at least 10% of patents in that cluster
-- that are in that field
filter_fields AS (
  SELECT
    cluster_id,
    subfield,
    subfield || ' (' || ROUND(percentage * 100, 1) || '%)' AS text,
    percentage,
    robotics_rank
  FROM
    ranked_fields
  WHERE robotics_rank <= 3
    OR percentage >= 0.1
  ORDER BY cluster_id, robotics_rank
)

SELECT
  cluster_id,
  ARRAY_AGG(STRUCT(subfield AS name, percentage) ORDER BY robotics_rank) AS robotics_fields, --noqa: L029
  STRING_AGG(text, ", ") AS robotics_fields_agg
FROM
  filter_fields
GROUP BY
  cluster_id
