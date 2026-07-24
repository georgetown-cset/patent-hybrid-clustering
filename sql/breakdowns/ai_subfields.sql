-- Get top AI subfields for every cluster that's AI-relevant
WITH
overall_filter AS (
  SELECT
    *
  FROM
    staging_patent_clusters.ai_pred
  WHERE pred_ai >= 0.1
),

unpivoted AS (
  SELECT
    *
  FROM
    overall_filter UNPIVOT(percentage FOR subfield IN (
      Physical_Sciences_and_Engineering_pred,
      Life_Sciences_pred,
      Security__eg_cybersecurity_pred,
      Transportation_pred,
      Industrial_and_Manufacturing_pred,
      Education_pred,
      Document_Mgt_and_Publishing_pred,
      Military_pred,
      Agricultural_pred,
      Computing_in_Government_pred,
      Personal_Devices_and_Computing_pred,
      Banking_and_Finance_pred,
      Telecommunications_pred,
      Networks__eg_social_IOT_etc_pred,
      Business_pred,
      Energy_Management_pred,
      Entertainment_pred,
      Nanotechnology_pred,
      Semiconductors_pred,
      Language_Processing_pred,
      Speech_Processing_pred,
      Knowledge_Representation_pred,
      Planning_and_Scheduling_pred,
      Control_pred,
      Distributed_AI_pred,
      Robotics_pred,
      Computer_Vision_pred,
      Analytics_and_Algorithms_pred,
      Measuring_and_Testing_pred,
      Spatial_and_Physical_Reasoning_pred,
      Logic_Based_Knowledge_Systems_pred,
      Fuzzy_Logic_pred,
      Bio_Inspired_Computing_pred,
      Markov_and_Bayesian_Models_pred,
      Ontology_Engineering_pred,
      Neural_Networks_pred,
      Search_Methods_pred
    ))
),

-- In addition to ranking here we also want to eliminate any fields that have 0 patents
-- We also want to change all our names to better names
ranked_fields AS (
  SELECT
    cluster_id,
    field_name AS subfield,
    percentage,
    ROW_NUMBER() OVER (PARTITION BY cluster_id ORDER BY percentage DESC) AS ai_rank
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
    ai_rank
  FROM
    ranked_fields
  WHERE ai_rank <= 3
    OR percentage >= 0.1
  ORDER BY cluster_id, ai_rank
)

SELECT
  cluster_id,
  ARRAY_AGG(STRUCT(subfield AS name, percentage) ORDER BY ai_rank) AS ai_fields, --noqa: L029
  STRING_AGG(text, ", ") AS ai_fields_agg
FROM
  filter_fields
GROUP BY
  cluster_id
