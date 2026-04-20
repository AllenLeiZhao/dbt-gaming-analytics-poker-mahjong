with af_installs as (
    select * from {{ ref('stg_af_installs') }}
),

ga4_events as (
    select * from {{ ref('stg_ga4__events') }}
),

af_cost as (
    select * from {{ ref('stg_af_cost') }}
),

-- Aggregate user behavior from GA4
user_behavior as (
    select
        user_pseudo_id,
        advertising_id,
        countif(event_name = 'e_1_game_start')          as total_sessions,
        countif(event_name like 'level_complete%')      as levels_completed,
        timestamp_micros(min(event_timestamp))          as first_seen_at,
        timestamp_micros(max(event_timestamp))          as last_seen_at,
        date_diff(
            date(timestamp_micros(max(event_timestamp))),
            date(timestamp_micros(min(event_timestamp))),
            day
        ) as days_active
    from ga4_events
    group by user_pseudo_id, advertising_id
),

-- Aggregate cost by media source
cost_by_source as (
    select
        media_source,
        sum(total_cost)     as total_cost,
        sum(total_revenue)  as total_revenue,
        sum(installs)       as paid_installs,
        avg(cpi)            as avg_cpi,
        avg(roi)            as avg_roi
    from af_cost
    group by media_source
),

-- Join AF installs with GA4 behavior via advertising_id
joined as (
    select
        af.appsflyer_id,
        af.advertising_id,
        af.installed_at,
        af.media_source,
        af.campaign,
        af.country_code,
        af.platform,
        af.is_retargeting,

        -- GA4 behavior metrics
        ga.total_sessions,
        ga.levels_completed,
        ga.days_active,
        ga.last_seen_at,

        -- Cost metrics
        c.total_cost,
        c.total_revenue,
        c.avg_cpi,
        c.avg_roi

    from af_installs af
    left join user_behavior ga
        on lower(af.advertising_id) = lower(ga.advertising_id)
    left join cost_by_source c
        on af.media_source = c.media_source
)

select * from joined