{{ config(enabled=false) }}

SELECT
    category,
    region,
    rating,
    waiting,
    rec_quality,
    rec_balanced,
    rec_convenience,
    cluster
FROM {{ ref('map_search') }}