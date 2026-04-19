with new_users as (
    select
        user_pseudo_id,
        event_date as cohort_date
    from {{ ref('stg_ga4__events') }}
    where event_name = 'first_open'
),

active_users as (
    select distinct
        user_pseudo_id,
        event_date
    from {{ ref('stg_ga4__events') }}
    where event_name = 'session_start'
)

select
    n.cohort_date,
    count(distinct n.user_pseudo_id)                                    as new_users,
    count(distinct case 
        when date_diff(parse_date('%Y%m%d', a1.event_date), 
                       parse_date('%Y%m%d', n.cohort_date), day) = 1 
        then a1.user_pseudo_id end)                                     as d1_retained,
    count(distinct case 
        when date_diff(parse_date('%Y%m%d', a7.event_date), 
                       parse_date('%Y%m%d', n.cohort_date), day) = 7 
        then a7.user_pseudo_id end)                                     as d7_retained,

    round(count(distinct case 
        when date_diff(parse_date('%Y%m%d', a1.event_date), 
                       parse_date('%Y%m%d', n.cohort_date), day) = 1 
        then a1.user_pseudo_id end) 
        / nullif(count(distinct n.user_pseudo_id), 0), 4)               as d1_retention_rate,
    round(count(distinct case 
        when date_diff(parse_date('%Y%m%d', a7.event_date), 
                       parse_date('%Y%m%d', n.cohort_date), day) = 7 
        then a7.user_pseudo_id end) 
        / nullif(count(distinct n.user_pseudo_id), 0), 4)               as d7_retention_rate

from new_users n
left join active_users a1
    on n.user_pseudo_id = a1.user_pseudo_id
left join active_users a7
    on n.user_pseudo_id = a7.user_pseudo_id
group by n.cohort_date
order by n.cohort_date