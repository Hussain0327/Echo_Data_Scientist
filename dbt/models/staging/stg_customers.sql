with source as (
    select * from {{ source('raw', 'stg_customers') }}
),

cleaned as (
    select
        customer_id,
        lower(trim(email)) as email,
        trim(coalesce(name, '')) as name,
        lower(trim(coalesce(segment, 'unknown'))) as segment,
        lower(trim(coalesce(plan_type, 'free'))) as plan_type,
        lower(trim(coalesce(acquisition_channel, 'direct'))) as acquisition_channel,
        cast(created_at as timestamp) as created_at,
        cast(coalesce(updated_at, created_at) as timestamp) as updated_at,
        current_timestamp as _loaded_at
    from source
    where customer_id is not null
)

select * from cleaned
