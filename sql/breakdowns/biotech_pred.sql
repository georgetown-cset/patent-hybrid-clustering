-- Find percentage biotechnology and biotechnology subcategories by cluster

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

/* Biotech prediction */
biotech_tab AS (
  SELECT
    patent_id,
    family_id,
    -- If the patent is in the biotech table, it's biotech
    1 AS biotech,
    CAST(Diagnosis_Sequencing_Bioinformatics AS INT64) AS Diagnosis_Sequencing_Bioinformatics,
    CAST(Product_Extraction_Purification_Synthesis AS INT64) AS Product_Extraction_Purification_Synthesis,
    CAST(Gene_Editing_Engineering AS INT64) AS Gene_Editing_Engineering,
    CAST(Physical_Apparatus_Robotics AS INT64) AS Physical_Apparatus_Robotics,
    CAST(Microorganisms_Bacteria_Viruses AS INT64) AS Microorganisms_Bacteria_Viruses,
    CAST(Tissues_Cells AS INT64) AS Tissues_Cells,
    CAST(Compounds AS INT64) AS Compounds,
    CAST(Genes_and_Genomes AS INT64) AS Genes_and_Genomes,
    CAST(Health AS INT64) AS Health,
    CAST(Industrial AS INT64) AS Industrial,
    CAST(Agriculture AS INT64) AS Agriculture,
    CAST(Food AS INT64) AS Food,
    CAST(Energy AS INT64) AS Energy,
    CAST(Military_Defense AS INT64) AS Military_Defense
  FROM
    staging_unified_patents.biotech_patents
),

/* Merge clusters and robotics predictions, including each only once per family id */
merged AS (
  SELECT DISTINCT
    clusters.family_id,
    cluster_id,
    COALESCE(biotech, 0) AS biotech,
    Diagnosis_Sequencing_Bioinformatics,
    Product_Extraction_Purification_Synthesis,
    Gene_Editing_Engineering,
    Physical_Apparatus_Robotics,
    Microorganisms_Bacteria_Viruses,
    Tissues_Cells,
    Compounds,
    Genes_and_Genomes,
    Health,
    Industrial,
    Agriculture,
    Food,
    Energy,
    Military_Defense
  FROM
    clusters
  LEFT JOIN
    biotech_tab
    USING
      (patent_id)
)

SELECT
  cluster_id,
  SUM(biotech) / NULLIF(COUNT(family_id), 0) AS pred_biotech,
  SUM(Diagnosis_Sequencing_Bioinformatics) / NULLIF(COUNT(family_id), 0) AS Diagnosis_Sequencing_Bioinformatics_pred,
  SUM(
    Product_Extraction_Purification_Synthesis
  ) / NULLIF(COUNT(family_id), 0) AS Product_Extraction_Purification_Synthesis_pred,
  SUM(Gene_Editing_Engineering) / NULLIF(COUNT(family_id), 0) AS Gene_Editing_Engineering_pred,
  SUM(Physical_Apparatus_Robotics) / NULLIF(COUNT(family_id), 0) AS Physical_Apparatus_Robotics_pred,
  SUM(Microorganisms_Bacteria_Viruses) / NULLIF(COUNT(family_id), 0) AS Microorganisms_Bacteria_Viruses_pred,
  SUM(Tissues_Cells) / NULLIF(COUNT(family_id), 0) AS Tissues_Cells_pred,
  SUM(Compounds) / NULLIF(COUNT(family_id), 0) AS Compounds_pred,
  SUM(Genes_and_Genomes) / NULLIF(COUNT(family_id), 0) AS Genes_and_Genomes_pred,
  SUM(Health) / NULLIF(COUNT(family_id), 0) AS Health_pred,
  SUM(Industrial) / NULLIF(COUNT(family_id), 0) AS Industrial_pred,
  SUM(Agriculture) / NULLIF(COUNT(family_id), 0) AS Agriculture_pred,
  SUM(Food) / NULLIF(COUNT(family_id), 0) AS Food_pred,
  SUM(Energy) / NULLIF(COUNT(family_id), 0) AS Energy_pred,
  SUM(Military_Defense) / NULLIF(COUNT(family_id), 0) AS Military_Defense_pred
FROM
  merged
GROUP BY
  cluster_id
ORDER BY
  cluster_id
