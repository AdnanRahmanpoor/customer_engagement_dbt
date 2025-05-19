{{ config(materialized='table') }}

with purchases as (
    select
        e.customer_id,
        e.event_time,
        e.event_type,
        c.signup_channel,
    from {{ ref('stg_events') }} e
    left join {{ ref('stg_customers') }} c on e.customer_id = c.customer_id
    where event_type = 'purchase'
),

aggregated as (
    select
        customer_id,
        signup_channel,
        count(*) as total_purchases
    from purchases
    group by customer_id, signup_channel
)

select * from aggregated