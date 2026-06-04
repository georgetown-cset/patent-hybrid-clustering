-- Check that searchable_organizations fields that shouldn't be null aren't
SELECT LOGICAL_AND(org_name IS NOT NULL)
FROM
  staging_map_of_patents.searchable_organizations
