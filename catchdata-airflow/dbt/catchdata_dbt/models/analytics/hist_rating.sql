{{ config(materialized='incremental', unique_key='rating_history_id') }}

select
    {{ dbt_utils.generate_surrogate_key([
        'id',
        'rating',
        'update_time'
    ]) }}              as rating_history_id,
    id                 as restaurant_id,
    lag(rating) over (
        partition by id
        order by update_time
    )                  as prev_rating,
    rating             as new_rating,
    update_time        as changed_at,
    rating -
    lag(rating) over (
        partition by id
        order by update_time
    )                  as rating_diff
from raw_data.kakao_crawl

{% if is_incremental() %}
  where update_time >
        (select max(changed_at) from {{ this }})
{% endif %}