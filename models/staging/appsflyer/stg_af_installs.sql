with source as (
    select * from {{ source('appsflyer', 'raw_af_installs') }}
),

renamed as (
    select
        -- identifiers
        appsflyer_id,
        advertising_id,

        -- timestamps
        cast(install_time as timestamp)     as installed_at,

        -- attribution
        media_source,
        campaign,
        adset,
        ad,
        channel,

        -- device & geo
        country_code,
        platform,
        app_version,

        -- flags
        is_retargeting

    from source
)

select * from renamed