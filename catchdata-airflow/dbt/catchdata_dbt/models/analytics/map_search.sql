{{ config(enabled=false) }}

SELECT 
    A.id,
    A.place_name AS name,
    -- 주소 파싱 (Redshift SQL 문법)
    SPLIT_PART(A.address_name, ' ', 1) AS region,
    SPLIT_PART(A.address_name, ' ', 2) AS city,
    -- 카테고리 파싱
    SPLIT_PART(A.category_name, ' > ', REGEXP_COUNT(A.category_name, ' > ') + 1) AS category,
    A.x,
    A.y,
    B.waiting,
    A.rating,
    A.phone,
    A.img_url AS image_url,
    A.address_name AS address,
    B.rec_quality,
    B.rec_balanced,
    B.rec_convenience,
    C.cluster
FROM {{ source('raw_data_source', 'kakao_crawl') }} A
INNER JOIN {{ source('analytics_source', 'realtime_waiting') }} B ON A.id = B.id
LEFT JOIN {{ source('analytics_source', 'derived_features_base') }} C ON A.id = C.id