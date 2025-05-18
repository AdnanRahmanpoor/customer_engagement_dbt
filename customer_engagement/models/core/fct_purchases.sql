{{ config(materialized='table') }}

with purchases as (
    select
        customer_id,
        event_time,
        event_type
    from {{ ref('stg_events') }}
    where event_type = 'purchase'
),

aggregated as (
    select
        customer_id,
        count(*) as total_purchases
    from purchases
    group by customer_id
)

select * from aggregated