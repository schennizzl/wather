with name_rows as (
    select appid, game_name, ingested_at, source_file
    from {{ ref('stg_steam_game_online') }}
    where game_name is not null

    union all

    select appid, game_name, ingested_at, source_file
    from {{ ref('stg_steam_twitch_viewers') }}
    where game_name is not null

    union all

    select appid, game_name, ingested_at, source_file
    from {{ ref('stg_steam_twitch_channels') }}
    where game_name is not null
),
name_ranges as (
    select
        appid,
        game_name,
        min(ingested_at) as valid_from_ts,
        max(ingested_at) as last_seen_ts,
        max_by(source_file, ingested_at) as source_file
    from name_rows
    group by appid, game_name
)
select
    appid,
    game_name,
    valid_from_ts,
    lead(valid_from_ts) over (partition by appid order by valid_from_ts, game_name) as valid_to_ts,
    last_seen_ts,
    source_file
from name_ranges
