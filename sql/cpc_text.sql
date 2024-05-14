/*
Goal: get CPC text where available, or substitute IPC matches when not available.
Notes:
  - CPC/IPCs have been updated:
    - Both in Unified Patents, and in cpc_codes
  - Table creation has not run
    - The query is taking forever to run
    - Something about the current approach is slow
*/
CREATE OR REPLACE TABLE staging_patent_clusters.family_cpc_text AS (
  WITH stage AS (
    SELECT
      metadata_d_p_removed.family_id AS family_id,
      patent_id,
      REPLACE(cpcs, ' ', '') AS cpcs,
      REPLACE(ipcs, ' ', '') AS ipcs
    FROM staging_patent_clusters.metadata_d_p_removed
    INNER JOIN `unified_patents.classifications` USING (patent_id), UNNEST(cpcs) AS cpcs, UNNEST(ipcs) AS ipcs
  ),

  text_join AS (
    SELECT
      family_id,
      patent_id,
      cpc_td.code AS cpc_code,
      cpc_td.level AS cpc_level,
      RANK() OVER(ORDER BY cpc_td.level) AS cpc_rank,
      ipc_td.code AS ipc_code,
      ipc_td.level AS ipc_level,
      RANK() OVER(ORDER BY ipc_td.level) AS ipc_rank
    FROM stage
    LEFT JOIN `cpc_codes.all_cpc_titles` AS cpc_td ON(cpcs = cpc_td.code)
    LEFT JOIN `cpc_codes.all_cpc_titles` AS ipc_td ON(ipcs = ipc_td.code)
    WHERE (cpc_td.code IS NOT NULL OR ipc_td.code IS NOT NULL)
  ),

  cpc_aggregation AS (
    SELECT
      family_id,
      patent_id,
      cpc_code,
      ARRAY_AGG(cpc_level ORDER BY cpc_rank DESC) AS cpc_array
    FROM text_join
    GROUP BY family_id, patent_id, cpc_code
  ),

  ipc_aggregation AS (
    SELECT
      family_id,
      patent_id,
      ipc_code,
      ARRAY_AGG(ipc_level ORDER BY ipc_rank DESC) AS ipc_array
    FROM text_join
    GROUP BY family_id, patent_id, ipc_code
  ),

  cpc_filtering AS (
    SELECT
      family_id,
      patent_id,
      cpc_code,
      cpc_td.text AS cpc_text
    FROM cpc_aggregation, UNNEST(cpc_array) cpc_level WITH OFFSET ranking
    INNER JOIN `cpc_codes.all_cpc_titles` cpc_td ON(CONCAT(cpc_code, cpc_level) = CONCAT(cpc_td.code, cpc_td.level))
    WHERE ranking = 0
  ),

  ipc_filtering AS (
    SELECT
      family_id,
      patent_id,
      ipc_td.text AS ipc_text
    FROM ipc_aggregation, UNNEST(ipc_array) ipc_level WITH OFFSET ranking
    INNER JOIN `cpc_codes.all_cpc_titles` ipc_td ON(CONCAT(ipc_code, ipc_level) = CONCAT(ipc_td.code, ipc_td.level))
    WHERE ranking = 0
  ),

  family_stage AS (
    SELECT
      family_id,
      STRING_AGG(DISTINCT cpc_text, ". ") AS cpc_text,
      STRING_AGG(DISTINCT ipc_text, ". ") AS ipc_text
    FROM ipc_filtering
    FULL OUTER JOIN cpc_filtering USING(family_id)
    GROUP BY family_id
  )

  SELECT
    family_id,
    COALESCE(cpc_text, ipc_text) AS cpc_text
  FROM family_stage
  ORDER BY family_id
  LIMIT 20
)
