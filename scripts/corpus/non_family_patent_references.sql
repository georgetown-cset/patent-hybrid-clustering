WITH test AS (
  SELECT 
    ref.patent_id, 
    md.family_id AS family_id, 
    patent_reference, 
    md2.family_id AS reference_family
  FROM`unified_patents.references` ref
  LEFT JOIN `unified_patents.metadata` md USING(patent_id)
  LEFT JOIN `unified_patents.metadata` md2 ON(ref.patent_reference=md2.patent_id)
), totals AS (
  SELECT 
    COUNT(DISTINCT family_id) AS total_families
  FROM test
), counts AS (
  SELECT 
    COUNT(DISTINCT family_id) AS total_outside_reference_families
  FROM test
  WHERE reference_family != family_id
)
SELECT 
  ROUND((total_outside_reference_families/total_families)*100,2)
FROM counts
CROSS JOIN totals 