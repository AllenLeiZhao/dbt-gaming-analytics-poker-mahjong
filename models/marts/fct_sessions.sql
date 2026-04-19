select
    event_date,
    count(*) as total_sessions,
    count(distinct user_pseudo_id) as active_users,
    round(count(*) / count(distinct user_pseudo_id), 2) as sessions_per_user
from {{ ref('stg_ga4__events') }}
where event_name = 'session_start'
group by event_date
order by event_date