{{ config(materialized='table') }}

with daily_events as (
    select
        date(event_time) as activity_date,
        customer_id
    from {{ ref('stg_events') }}
    group by 1, 2
),

daily_active_users as (
    select
        activity_date,
        count(distinct customer_id) as dau
    from daily_events
    group by activity_date
)

select * from daily_active_users
order by activity_date