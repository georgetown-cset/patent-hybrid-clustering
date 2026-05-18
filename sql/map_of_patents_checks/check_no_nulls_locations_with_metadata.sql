-- Check that locations_with_metadata fields that shouldn't be null aren't
SELECT LOGICAL_AND(cluster_id IS NOT NULL)
  AND LOGICAL_AND(class_cat IS NOT NULL)
  AND LOGICAL_AND(NPF_all_years IS NOT NULL)
  AND LOGICAL_AND(cluster_title IS NOT NULL)
  AND LOGICAL_AND(cluster_summary IS NOT NULL)
  AND LOGICAL_AND(ai_pred IS NOT NULL)
  AND LOGICAL_AND(ro_pred IS NOT NULL)
  AND LOGICAL_AND(cyber_pred IS NOT NULL)
  AND LOGICAL_AND(semiconductor_pred IS NOT NULL)
  AND LOGICAL_AND(biotech_pred IS NOT NULL)
  AND LOGICAL_AND(age IS NOT NULL)
FROM
  staging_map_of_patents.locations_with_metadata
