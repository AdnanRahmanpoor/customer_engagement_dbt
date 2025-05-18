{{ config(materialized='table') }}

with customers as (
    select
        customer_id,
        created_at
    from {{ ref('stg_customers') }}
),

events as (
    select
        customer_id,
        max(event_time) as last_event_time
    from {{ ref('stg_events') }}
    group by customer_id
),

lifecycle as (
    select
        c.customer_id,
        c.created_at as signup_date,
        e.last_event_time,
        date_diff(current_date(), date(e.last_event_time), day) as days_since_last_activity,
        case
            when timestamp(e.last_event_time) >= timestamp_sub(timestamp(current_date()), interval 7 day) then 'active'
            when timestamp(e.last_event_time) >= timestamp_sub(timestamp(current_date()), interval 30 day) then 'dormant'
            else 'churned'
        end as status
    from customers c
    left join events e on c.customer_id = e.customer_id
)

select * from lifecycle