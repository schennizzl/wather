select
    cast(json_extract_scalar(json_parse(payload), '$.appid') as bigint) as appid,
    json_extract_scalar(json_parse(payload), '$.game_name') as game_name,
    json_extract_scalar(json_parse(payload), '$.twitch_lookup_name') as twitch_lookup_name,
    json_extract_scalar(json_parse(payload), '$.twitch_category_id') as twitch_category_id,
    json_extract_scalar(json_parse(payload), '$.twitch_category_name') as twitch_category_name,
    cast(json_extract_scalar(json_parse(payload), '$.approx_total_viewers') as integer) as approx_total_viewers,
    cast(json_extract_scalar(json_parse(payload), '$.live_channels') as integer) as live_channels,
    cast(json_extract_scalar(json_parse(payload), '$.pages_fetched') as integer) as pages_fetched,
    cast(json_extract_scalar(json_parse(payload), '$.is_partial') as boolean) as is_partial,
    cast(dt as date) as event_date,
    cast(hour as integer) as event_hour,
    cast(replace(substr(ingested_at, 1, 19), 'T', ' ') as timestamp(3)) as ingested_at,
    cast(source_file as varchar) as source_file
from {{ source('raw', 'twitch_viewers_files') }}
where payload is not null
