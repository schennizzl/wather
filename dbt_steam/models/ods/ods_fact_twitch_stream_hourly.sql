with dedup as (
    select
        appid,
        twitch_category_id,
        broadcaster_id,
        title,
        language,
        started_at,
        thumbnail_url,
        is_mature,
        viewer_count,
        pages_fetched,
        is_partial,
        event_date,
        event_hour,
        cast(date_add('hour', event_hour, cast(event_date as timestamp)) as timestamp(3)) as event_ts_hour,
        source_file,
        ingested_at,
        row_number() over (
            partition by appid, coalesce(broadcaster_id, ''), event_date, event_hour
            order by ingested_at desc, source_file desc
        ) as rn
    from {{ ref('stg_steam_twitch_channels') }}
)
select
    cast(appid as varchar) || '|' || coalesce(broadcaster_id, '') || '|' || cast(event_ts_hour as varchar) as twitch_stream_event_id,
    appid,
    twitch_category_id,
    broadcaster_id,
    title,
    language,
    started_at,
    thumbnail_url,
    is_mature,
    viewer_count,
    pages_fetched,
    is_partial,
    event_date,
    event_hour,
    event_ts_hour,
    source_file,
    ingested_at
from dedup
where rn = 1
