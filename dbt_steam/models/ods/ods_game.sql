with sightings as (
    select appid, ingested_at from {{ ref('stg_steam_game_online') }}
    union all
    select appid, ingested_at from {{ ref('stg_steam_twitch_viewers') }}
    union all
    select appid, ingested_at from {{ ref('stg_steam_twitch_channels') }}
)
select
    appid,
    min(ingested_at) as first_seen_at,
    max(ingested_at) as last_seen_at
from sightings
group by appid
