-- Find percentage cybersecurity and cybersecurity subcategories by cluster

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

/* Cybersecurity prediction */
cybersecurity_tab AS (
  SELECT
    patent_id,
    family_id,
    -- If the patent is in the cybersecurity table, it's cybersecurity
    1 AS cybersecurity,
    CAST(Communications_Cryptography AS INT64) AS Communications_Cryptography,
    CAST(Net_Analysis_Forensics_Attack_Detection AS INT64) AS Net_Analysis_Forensics_Attack_Detection,
    CAST(Authentication_Authorization AS INT64) AS Authentication_Authorization,
    CAST(Vulnerability_Discovery_Threat_Mitigation AS INT64) AS Vulnerability_Discovery_Threat_Mitigation,
    CAST(Attack_Interdiction_Recovery_Restoration AS INT64) AS Attack_Interdiction_Recovery_Restoration,
    CAST(Asset_Management_Policy_Enforcement AS INT64) AS Asset_Management_Policy_Enforcement,
    CAST(Other_General AS INT64) AS Other_General
  FROM
    unified_patents.cybersecurity_patents
),

/* Merge clusters and cybersecurity predictions, including each only once per family id */
merged AS (
  SELECT DISTINCT
    clusters.family_id,
    cluster_id,
    COALESCE(cybersecurity, 0) AS cybersecurity,
    Communications_Cryptography,
    Net_Analysis_Forensics_Attack_Detection,
    Authentication_Authorization,
    Vulnerability_Discovery_Threat_Mitigation,
    Attack_Interdiction_Recovery_Restoration,
    Asset_Management_Policy_Enforcement,
    Other_General
  FROM
    clusters
  LEFT JOIN
    cybersecurity_tab
    USING
      (patent_id)
)

SELECT
  cluster_id,
  SUM(cybersecurity) / NULLIF(COUNT(family_id), 0) AS pred_cybersecurity,
  SUM(Communications_Cryptography) / NULLIF(COUNT(family_id), 0) AS Communications_Cryptography_pred,
  SUM(
    Net_Analysis_Forensics_Attack_Detection
  ) / NULLIF(COUNT(family_id), 0) AS Net_Analysis_Forensics_Attack_Detection_pred,
  SUM(Authentication_Authorization) / NULLIF(COUNT(family_id), 0) AS Authentication_Authorization_pred,
  SUM(
    Vulnerability_Discovery_Threat_Mitigation
  ) / NULLIF(COUNT(family_id), 0) AS Vulnerability_Discovery_Threat_Mitigation_pred,
  SUM(
    Attack_Interdiction_Recovery_Restoration
  ) / NULLIF(COUNT(family_id), 0) AS Attack_Interdiction_Recovery_Restoration_pred,
  SUM(Asset_Management_Policy_Enforcement) / NULLIF(COUNT(family_id), 0) AS Asset_Management_Policy_Enforcement_pred,
  SUM(Other_General) / NULLIF(COUNT(family_id), 0) AS Other_General_pred
FROM
  merged
GROUP BY
  cluster_id
ORDER BY
  cluster_id
