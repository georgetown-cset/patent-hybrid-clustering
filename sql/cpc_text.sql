/*
Goal: get CPC text where available, or substitute IPC matches when not available. 
Notes: 
  - ARRAY_CONCAT_AGG does not allow DISTINCT. 
  - Therefore, first UNNEST then ARRAY_AGG at the family level. 
  - Takes around 1 minute to run. 
*/
WITH stage AS (
  SELECT 
    md.family_id AS family_id, 
    patent_id, 
    REPLACE(cpcs, ' ', '') AS cpcs, 
    REPLACE(ipcs, ' ', '') AS ipcs
  FROM `staging_patent_clusters.metadata_d_p_removed` md
  INNER JOIN `unified_patents.classifications` USING(patent_id), UNNEST(cpcs) cpcs, UNNEST(ipcs) ipcs
), text_join AS (
  SELECT 
    family_id, 
    CONCAT(IFNULL(cpc_td.title, ''), IFNULL(cpc_td.description, '')) AS cpc_text, 
    CONCAT(IFNULL(ipc_td.title, ''), IFNULL(ipc_td.description, '')) AS ipc_text
  FROM stage 
  JOIN `cpc_codes.cpc_title_descriptions` cpc_td ON(cpcs=cpc_td.code)
  LEFT JOIN `cpc_codes.cpc_title_descriptions` ipc_td ON(ipcs=ipc_td.code)
), family_agg AS (
  SELECT 
    family_id, 
    STRING_AGG(DISTINCT cpc_text ORDER BY cpc_text) AS cpc_text, 
    STRING_AGG(DISTINCT ipc_text ORDER BY ipc_text) AS ipc_text  
  FROM text_join
  GROUP BY family_id 
)
SELECT 
  family_id, 
  COALESCE(cpc_text, ipc_text) AS text 
FROM family_agg