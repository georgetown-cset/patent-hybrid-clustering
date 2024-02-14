/*
Following feedback from Patrick & team discussion: 
Remove U.S. design/plant patents but retain all other patents
that may be in the family. 
Create new table to then use for other metadata counts. 
*/
CREATE OR REPLACE TABLE staging_patent_clusters.metadata_d_p_removed AS (
  SELECT 
  *
  FROM `unified_patents.metadata`
  WHERE 
    NOT ((REGEXP_CONTAINS(SPLIT(patent_id, '-')[OFFSET(2)], 'P') 
    OR REGEXP_CONTAINS(SPLIT(patent_id, '-')[OFFSET(2)], 'S'))
    AND SPLIT(patent_id, '-')[OFFSET(0)] = 'US')
)