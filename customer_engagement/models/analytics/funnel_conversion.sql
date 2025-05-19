{{ config(materialized='table') }}

with signup as (
    select distinct customer_id
    from {{ ref('stg_events') }}
    where event_type = 'signup'
),

login as (
    select distinct customer_id
    from {{ ref('stg_events') }}
    where event_type = 'login'
),

purchase as (
    select distinct customer_id
    from {{ ref('stg_events') }}
    where event_type = 'purchase'
),

final as (
    select
        'signup' as stage,
        count(*) as users
    from signup
    union all
    select
        'login' as stage,
        count(*) as users
    from login
    union all
    select
        'purchase' as stage,
        count(*) as users
    from purchase
)

select * from final