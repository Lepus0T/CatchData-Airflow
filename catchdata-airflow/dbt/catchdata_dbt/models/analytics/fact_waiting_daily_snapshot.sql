{{ config(
    materialized = 'incremental',
    unique_key = ['snapshot_date', 'restaurant_id']
) }}

with base as (

    select
        -- KST 기준 일자 (타임존 명확화)
        date_trunc(
            'day',
            measured_at AT TIME ZONE 'Asia/Seoul'
        )::date                     as snapshot_date,

        restaurant_id,
        waiting_count
    from {{ ref('fact_waiting_event') }}

    {% if is_incremental() %}
    where measured_at >
        (
            select coalesce(max(snapshot_date), '1900-01-01')
            from {{ this }}
        )
    {% endif %}

)

select
    snapshot_date,                     -- 기준 일자
    restaurant_id,                     -- 식당 ID
    avg(waiting_count)     as avg_waiting,          -- 평균 웨이팅 수
    max(waiting_count)     as max_waiting,          -- 최대 웨이팅 수
    count(*)               as waiting_event_count   -- 웨이팅 변경 횟수
from base
group by 1, 2
