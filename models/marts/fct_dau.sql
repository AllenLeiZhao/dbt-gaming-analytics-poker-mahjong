select 
    event_date,
    count (distinct user_pseudo_id) as dau
from {{ ref('stg_ga4__events') }}
where event_name='session_start'
group by event_date
order by event_date