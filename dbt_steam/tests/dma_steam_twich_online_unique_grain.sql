select
    steam_app_id,
    coalesce(cast(twitch_category_id as varchar), '<null>') as twitch_category_id,
    coalesce(cast(broadcaster_id as varchar), '<null>') as broadcaster_id,
    event_ts_hour,
    count(*) as row_count
from {{ ref('dma_steam_twich_online') }}
group by 1, 2, 3, 4
having count(*) > 1
