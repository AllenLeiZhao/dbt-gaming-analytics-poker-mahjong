with source as (
    select * from {{ source('appsflyer', 'raw_af_events') }}
),

renamed as (
    select
        -- identifiers
        appsflyer_id,
        advertising_id,

        -- timestamps
        cast(install_time as timestamp)     as installed_at,
        cast(event_time as timestamp)       as event_at,

        -- event details
        event_name,
        event_value,
        cast(event_revenue as float64)      as event_revenue,
        event_revenue_currency,

        -- attribution
        media_source,
        campaign,

        -- device & geo
        country_code,
        platform

    from source
)

select * from renamed