with lookup_rows as (
    select appid, twitch_lookup_name, ingested_at
    from {{ ref('stg_steam_twitch_viewers') }}
    where twitch_lookup_name is not null

    union all

    select appid, twitch_lookup_name, ingested_at
    from {{ ref('stg_steam_twitch_channels') }}
    where twitch_lookup_name is not null
),
lookup_ranges as (
    select
        appid,
        twitch_lookup_name,
        min(ingested_at) as valid_from_ts,
        max(ingested_at) as last_seen_ts
    from lookup_rows
    group by appid, twitch_lookup_name
)
select
    appid,
    twitch_lookup_name,
    valid_from_ts,
    lead(valid_from_ts) over (partition by appid order by valid_from_ts, twitch_lookup_name) as valid_to_ts,
    last_seen_ts
from lookup_ranges
