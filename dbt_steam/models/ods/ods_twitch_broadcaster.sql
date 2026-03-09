select
    broadcaster_id,
    min(ingested_at) as first_seen_at,
    max(ingested_at) as last_seen_at
from {{ ref('stg_steam_twitch_channels') }}
where broadcaster_id is not null
group by broadcaster_id
