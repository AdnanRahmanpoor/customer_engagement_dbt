{{ config(materialized='table') }}

with signups as (
    select
        customer_id,
        date_trunc(date(created_at), month) as signup_month
    from {{ ref('stg_customers') }}
),

activity as (
    select
        customer_id,
        date_trunc(date(event_time), month) as activity_month
    from {{ ref('stg_events') }}
    where event_type = 'login'
),

joined as (
    select
        s.customer_id,
        s.signup_month,
        a.activity_month,
        date_diff(date(a.activity_month), date(s.signup_month), month) as months_since_signup
    from signups s
    join activity a
      on s.customer_id = a.customer_id
    where date_diff(date(a.activity_month), date(s.signup_month), month) >= 0
),

cohort_counts as (
    select
        signup_month,
        months_since_signup,
        count(distinct customer_id) as active_users
    from joined
    group by signup_month, months_since_signup
),

base_counts as (
    select
        signup_month,
        count(distinct customer_id) as total_signups
    from signups
    group by signup_month
),

final as (
    select
        c.signup_month,
        c.months_since_signup,
        c.active_users,
        b.total_signups,
        round(c.active_users / b.total_signups, 3) as retention_rate
    from cohort_counts c
    join base_counts b on c.signup_month = b.signup_month
)

select * from final
order by signup_month, months_since_signup
