{{ config(materialized='view') }}

with source as (
  SELECT * FROM {{ source('customer_engagement', 'customers') }}
),

renamed as (
  select 
    customer_id,
    created_at,
    lower(country) as country,
    lower(device_type) as device_type,
    lower(signup_channel) as signup_channel
  from source
)

SELECT * FROM renamed
