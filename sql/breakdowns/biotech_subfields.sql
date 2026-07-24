-- Get top biotech subfields for every cluster that's biotech-relevant
WITH
overall_filter AS (
  SELECT
    *
  FROM
    staging_patent_clusters.biotech_pred
  WHERE pred_biotech >= 0.1
),

unpivoted AS (
  SELECT
    *
  FROM
    overall_filter UNPIVOT(percentage FOR subfield IN (
      Diagnosis_Sequencing_Bioinformatics_pred,
      Product_Extraction_Purification_Synthesis_pred,
      Gene_Editing_Engineering_pred,
      Physical_Apparatus_Robotics_pred,
      Microorganisms_Bacteria_Viruses_pred,
      Tissues_Cells_pred,
      Compounds_pred,
      Genes_and_Genomes_pred,
      Health_pred,
      Industrial_pred,
      Agriculture_pred,
      Food_pred,
      Energy_pred,
      Military_Defense_pred
    ))
),

-- In addition to ranking here we also want to eliminate any fields that have 0 patents
-- We also want to change all our names to better names
ranked_fields AS (
  SELECT
    cluster_id,
    field_name AS subfield,
    percentage,
    ROW_NUMBER() OVER (PARTITION BY cluster_id ORDER BY percentage DESC) AS biotech_rank
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
    biotech_rank
  FROM
    ranked_fields
  WHERE biotech_rank <= 3
    OR percentage >= 0.1
  ORDER BY cluster_id, biotech_rank
)

SELECT
  cluster_id,
  ARRAY_AGG(STRUCT(subfield AS name, percentage) ORDER BY biotech_rank) AS biotech_fields, --noqa: L029
  STRING_AGG(text, ", ") AS biotech_fields_agg
FROM
  filter_fields
GROUP BY
  cluster_id
