with source as (
    select * from {{ source('appsflyer', 'raw_af_cost') }}
),

renamed as (
    select
        -- date & attribution
        cast(date as date)          as date,
        media_source_pid            as media_source,
        campaign_c                  as campaign,

        -- volume metrics
        impressions,
        clicks,
        installs,
        ctr,
        conversion_rate,

        -- cost & revenue
        total_cost,
        total_revenue,
        average_ecpi                as cpi,
        roi,
        arpu,

        -- level funnel
        level_complete_1_unique_users,
        level_complete_2_unique_users,
        level_complete_3_unique_users,
        level_complete_4_unique_users,
        level_complete_5_unique_users

    from source
    where total_cost > 0
        or installs > 0
)

select * from renamed