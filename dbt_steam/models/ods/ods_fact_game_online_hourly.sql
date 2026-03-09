with dedup as (
    select
        appid,
        current_players,
        event_date,
        event_hour,
        cast(date_add('hour', event_hour, cast(event_date as timestamp)) as timestamp(3)) as event_ts_hour,
        source_file,
        ingested_at,
        row_number() over (
            partition by appid, event_date, event_hour
            order by ingested_at desc, source_file desc
        ) as rn
    from {{ ref('stg_steam_game_online') }}
)
select
    to_hex(sha256(to_utf8(cast(appid as varchar) || cast(event_ts_hour as varchar)))) as game_online_event_id,
    appid,
    current_players,
    event_date,
    event_hour,
    event_ts_hour,
    source_file,
    ingested_at
from dedup
where rn = 1
