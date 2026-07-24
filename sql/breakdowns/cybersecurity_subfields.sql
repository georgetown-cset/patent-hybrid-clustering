-- Get top cybersecurity subfields for every cluster that's cybersecurity-relevant
WITH
overall_filter AS (
  SELECT
    *
  FROM
    staging_patent_clusters.cybersecurity_pred
  WHERE pred_cybersecurity >= 0.1
),

unpivoted AS (
  SELECT
    *
  FROM
    overall_filter UNPIVOT(percentage FOR subfield IN (
      Communications_Cryptography_pred,
      Net_Analysis_Forensics_Attack_Detection_pred,
      Authentication_Authorization_pred,
      Vulnerability_Discovery_Threat_Mitigation_pred,
      Attack_Interdiction_Recovery_Restoration_pred,
      Asset_Management_Policy_Enforcement_pred
    ))
),

-- In addition to ranking here we also want to eliminate any fields that have 0 patents
-- We also want to change all our names to better names
ranked_fields AS (
  SELECT
    cluster_id,
    field_name AS subfield,
    percentage,
    ROW_NUMBER() OVER (PARTITION BY cluster_id ORDER BY percentage DESC) AS cyber_rank
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
    cyber_rank
  FROM
    ranked_fields
  WHERE cyber_rank <= 3
    OR percentage >= 0.1
  ORDER BY cluster_id, cyber_rank
)

SELECT
  cluster_id,
  ARRAY_AGG(STRUCT(subfield AS name, percentage) ORDER BY cyber_rank) AS cybersecurity_fields, --noqa: L029
  STRING_AGG(text, ", ") AS cybersecurity_fields_agg
FROM
  filter_fields
GROUP BY
  cluster_id
