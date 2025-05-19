{{ config(materialized='table') }}

with daily_events as (
    select
        date(e.event_time) as activity_date,
        e.customer_id,
        c.signup_channel
    from {{ ref('stg_events') }} e
    left join {{ ref('stg_customers') }} c on e.customer_id = c.customer_id
    group by 1, 2, 3
),

daily_active_users as (
    select
        activity_date,
        signup_channel,
        count(distinct customer_id) as dau
    from daily_events
    group by activity_date, signup_channel
)

select * from daily_active_users
order by activity_date, signup_channel