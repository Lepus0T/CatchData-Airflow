{{ config(materialized='incremental', unique_key='name_history_id') }}

select
    {{ dbt_utils.generate_surrogate_key([
        'id',
        'place_name',
        'update_time'
    ]) }}                 as name_history_id,
    id                    as restaurant_id,
    lag(place_name) over (
        partition by id
        order by update_time
    )                     as prev_name,
    place_name            as new_name,
    update_time           as changed_at
from raw_data.kakao_crawl

{% if is_incremental() %}
  where update_time >
        (select max(changed_at) from {{ this }})
{% endif %}