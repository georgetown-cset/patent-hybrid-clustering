SELECT
    patent_id,
    COALESCE(family_id, 'X-' || patent_id), AS family_id,
    title_original,
    abstract_original
FROM `staging_patent_clusters.new_metadata`
WHERE
    title is null
    and abstract is null
    and title_original is not null
    and abstract_original is not null
