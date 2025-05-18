{{ config(materialized='view') }}

with source as (
    select * from {{ source('customer_engagement', 'events') }}
    ),

renamed as (
    select
        event_id,
        customer_id,
        lower(event_type) as event_type,
        event_time
    from source
    )

select * from renamed