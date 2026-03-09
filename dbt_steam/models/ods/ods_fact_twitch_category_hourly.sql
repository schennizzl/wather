with dedup as (
    select
        appid,
        twitch_category_id,
        approx_total_viewers,
        live_channels,
        pages_fetched,
        is_partial,
        event_date,
        event_hour,
        cast(date_add('hour', event_hour, cast(event_date as timestamp)) as timestamp(3)) as event_ts_hour,
        source_file,
        ingested_at,
        row_number() over (
            partition by appid, coalesce(twitch_category_id, ''), event_date, event_hour
            order by ingested_at desc, source_file desc
        ) as rn
    from {{ ref('stg_steam_twitch_viewers') }}
)
select
    to_hex(sha256(to_utf8(cast(appid as varchar) || cast(twitch_category_id as varchar) || cast(event_ts_hour as varchar)))) as twitch_category_event_id,
    appid,
    twitch_category_id,
    approx_total_viewers,
    live_channels,
    pages_fetched,
    is_partial,
    event_date,
    event_hour,
    event_ts_hour,
    source_file,
    ingested_at
from dedup
where rn = 1
