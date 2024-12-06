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

/* Robotics prediction */
robotics_tab AS (
  SELECT
    patent_id,
    family_id,
    -- If the patent is in the robotics table, it's robotics
    1 as robotics,
    CAST(Robot_Type__Aerospace AS INT64) AS Robot_Type__Aerospace,
    CAST(Robot_Type__Agriculture AS INT64) AS Robot_Type__Agriculture,
    CAST(Robot_Type__Consumer AS INT64) AS Robot_Type__Consumer,
    CAST(Robot_Type__Education AS INT64) AS Robot_Type__Education,
    CAST(Robot_Type__Entertainment AS INT64) AS Robot_Type__Entertainment,
    CAST(Robot_Type__Humanoid_Exoskeleton AS INT64) AS Robot_Type__Humanoid_Exoskeleton,
    CAST(Robot_Type__Industrial AS INT64) AS Robot_Type__Industrial,
    CAST(Robot_Type__Medical AS INT64) AS Robot_Type__Medical,
    CAST(Robot_Type__Military_Security AS INT64) AS Robot_Type__Military_Security,
    CAST(Robot_Type__Telepresence AS INT64) AS Robot_Type__Telepresence,
    CAST(Robot_Type__Transportation AS INT64) AS Robot_Type__Transportation,
    CAST(Robot_Type__Underwater AS INT64) AS Robot_Type__Underwater,
    CAST(Robot_Feature__Artificial_Intelligence AS INT64) AS Robot_Feature__Artificial_Intelligence,
    CAST(Robot_Feature__Physical_Components AS INT64) AS Robot_Feature__Physical_Components,
    CAST(Robot_Feature__Control_Systems AS INT64) AS Robot_Feature__Control_Systems,
    CAST(Robot_Feature__Sensors_Measurement AS INT64) AS Robot_Feature__Sensors_Measurement,
    CAST(Robot_Feature__Speech AS INT64) AS Robot_Feature__Speech,
    CAST(Robot_Feature__Vision AS INT64) AS Robot_Feature__Vision
  FROM
    unified_patents.robotics_patents
),

/* Merge clusters and robotics predictions, including each only once per family id */ merged AS (
  SELECT
    DISTINCT
    clusters.family_id,
    cluster_id,
    COALESCE(robotics, 0) as robotics,
    Robot_Type__Aerospace,
    Robot_Type__Agriculture,
    Robot_Type__Consumer,
    Robot_Type__Education,
    Robot_Type__Entertainment,
    Robot_Type__Humanoid_Exoskeleton,
    Robot_Type__Industrial,
    Robot_Type__Medical,
    Robot_Type__Military_Security,
    Robot_Type__Telepresence,
    Robot_Type__Transportation,
    Robot_Type__Underwater,
    Robot_Feature__Artificial_Intelligence,
    Robot_Feature__Physical_Components,
    Robot_Feature__Control_Systems,
    Robot_Feature__Sensors_Measurement,
    Robot_Feature__Speech,
    Robot_Feature__Vision
  FROM
    clusters
  LEFT JOIN
    robotics_tab
  USING
    (patent_id)
)
SELECT
  cluster_id,
  SUM(robotics)/NULLIF(COUNT(family_id), 0) AS pred_robotics,
  SUM(Robot_Type__Aerospace)/NULLIF(COUNT(family_id), 0) AS Robot_Type__Aerospace_pred,
  SUM(Robot_Type__Agriculture)/NULLIF(COUNT(family_id), 0) AS Robot_Type__Agriculture_pred,
  SUM(Robot_Type__Consumer)/NULLIF(COUNT(family_id), 0) AS Robot_Type__Consumer_pred,
  SUM(Robot_Type__Education)/NULLIF(COUNT(family_id), 0) AS Robot_Type__Education_pred,
  SUM(Robot_Type__Entertainment)/NULLIF(COUNT(family_id), 0) AS Robot_Type__Entertainment_pred,
  SUM(Robot_Type__Humanoid_Exoskeleton)/NULLIF(COUNT(family_id), 0) AS Robot_Type__Humanoid_Exoskeleton_pred,
  SUM(Robot_Type__Industrial)/NULLIF(COUNT(family_id), 0) AS Robot_Type__Industrial_pred,
  SUM(Robot_Type__Medical)/NULLIF(COUNT(family_id), 0) AS Robot_Type__Medical_pred,
  SUM(Robot_Type__Military_Security)/NULLIF(COUNT(family_id), 0) AS Robot_Type__Military_Security_pred,
  SUM(Robot_Type__Telepresence)/NULLIF(COUNT(family_id), 0) AS Robot_Type__Telepresence_pred,
  SUM(Robot_Type__Transportation)/NULLIF(COUNT(family_id), 0) AS Robot_Type__Transportation_pred,
  SUM(Robot_Type__Underwater)/NULLIF(COUNT(family_id), 0) AS Robot_Type__Underwater_pred,
  SUM(Robot_Feature__Artificial_Intelligence)/NULLIF(COUNT(family_id), 0) AS Robot_Feature__Artificial_Intelligence_pred,
  SUM(Robot_Feature__Physical_Components)/NULLIF(COUNT(family_id), 0) AS Robot_Feature__Physical_Components_pred,
  SUM(Robot_Feature__Control_Systems)/NULLIF(COUNT(family_id), 0) AS Robot_Feature__Control_Systems_pred,
  SUM(Robot_Feature__Sensors_Measurement)/NULLIF(COUNT(family_id), 0) AS Robot_Feature__Sensors_Measurement_pred,
  SUM(Robot_Feature__Speech)/NULLIF(COUNT(family_id), 0) AS Robot_Feature__Speech_pred,
  SUM(Robot_Feature__Vision)/NULLIF(COUNT(family_id), 0) AS Robot_Feature__Vision_pred
FROM
  merged
GROUP BY
  cluster_id
ORDER BY
  cluster_id