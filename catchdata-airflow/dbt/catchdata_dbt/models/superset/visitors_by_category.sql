{{ config(enabled=false) }}

SELECT
    base.id,
    base.time0, base.time1, base.time2, base.time3, base.time4, 
    base.time5, base.time6, base.time7, base.time8, base.time9, 
    base.time10, base.time11, base.time12, base.time13, base.time14, 
    base.time15, base.time16, base.time17, base.time18, base.time19, 
    base.time20, base.time21, base.time22, base.time23,
    map.category
FROM {{ ref('derived_features_base') }} as base
INNER JOIN {{ ref('map_search') }} as map 
    ON base.id = map.id