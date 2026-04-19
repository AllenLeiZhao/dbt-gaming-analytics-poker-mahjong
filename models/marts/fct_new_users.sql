select
    event_date,
    count(distinct user_pseudo_id) as new_users
from {{ ref('stg_ga4__events') }}
where event_name = 'first_open'
group by event_date
order by event_date