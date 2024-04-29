/*
Goal: get CPC text where available, or substitute IPC matches when not available.
Notes:
  - ARRAY_CONCAT_AGG does not allow DISTINCT.
  - Therefore, first UNNEST then STRING_AGG at the family level.
  - Takes around 1 minute to run.
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
      CONCAT(COALESCE(TRIM(cpc_td.title), ''), " ", COALESCE(TRIM(cpc_td.description), '')) AS cpc_text,
      CONCAT(COALESCE(TRIM(ipc_td.title), ''), " ", COALESCE(TRIM(ipc_td.description), '')) AS ipc_text
    FROM stage
    LEFT JOIN `cpc_codes.cpc_title_descriptions` AS cpc_td ON(cpcs = cpc_td.code)
    LEFT JOIN `cpc_codes.cpc_title_descriptions` AS ipc_td ON(ipcs = ipc_td.code)
    WHERE (cpc_td.code IS NOT NULL OR ipc_td.code IS NOT NULL)
  ),

  family_agg AS (
    SELECT
      family_id,
      STRING_AGG(DISTINCT cpc_text, ". " ORDER BY cpc_text) AS cpc_text,
      STRING_AGG(DISTINCT ipc_text, ". " ORDER BY ipc_text) AS ipc_text
    FROM text_join
    GROUP BY family_id
  )

  SELECT
    family_id,
    COALESCE(cpc_text, ipc_text) AS text
  FROM family_agg
)