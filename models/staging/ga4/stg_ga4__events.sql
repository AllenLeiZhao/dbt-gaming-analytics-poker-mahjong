select
    event_date,
    event_name,
    event_timestamp,
    user_pseudo_id,
    platform,
    geo.country         as country,
    device.category     as device_category,
    
    (select value.int_value 
     from unnest(event_params) 
     where key = 'ga_session_id')      as session_id,
     
    (select value.int_value 
     from unnest(event_params) 
     where key = 'ga_session_number')  as session_number,
     
    (select value.int_value 
     from unnest(event_params) 
     where key = 'engagement_time_msec') as engagement_time_msec,
     
    (select value.string_value 
     from unnest(event_params) 
     where key = 'firebase_screen_class') as screen_class

from {{ source('ga4_poker_mahjong', 'events') }}