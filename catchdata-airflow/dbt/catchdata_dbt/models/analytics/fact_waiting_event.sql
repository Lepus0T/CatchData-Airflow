{{ config(
    materialized = 'incremental',
    unique_key = 'event_id'
) }}

with source as (

    select
        id as restaurant_id,
        waiting,
        calculation_timestamp as measured_at,
        lag(waiting) over (
            partition by id
            order by calculation_timestamp
        ) as prev_waiting
    from analytics.realtime_waiting

),

events as (

    select
        {{ dbt_utils.generate_surrogate_key([
            'restaurant_id',
            'measured_at'
        ]) }} as event_id,
        restaurant_id,
        waiting as waiting_count,
        measured_at
    from source
    where prev_waiting is null
    or waiting <> prev_waiting

)

select *
from events

{% if is_incremental() %}
where measured_at >
    (select coalesce(max(measured_at), '1900-01-01') from {{ this }})
{% endif %}
