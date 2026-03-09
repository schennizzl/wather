select
    cast(json_extract_scalar(payload, '$.appid') as bigint) as appid,
    json_extract_scalar(payload, '$.game_name') as game_name,
    json_extract_scalar(payload, '$.twitch_lookup_name') as twitch_lookup_name,
    json_extract_scalar(payload, '$.configured_twitch_category_id') as configured_twitch_category_id,
    json_extract_scalar(payload, '$.twitch_category_id') as twitch_category_id,
    json_extract_scalar(payload, '$.twitch_category_name') as twitch_category_name,
    json_extract_scalar(payload, '$.broadcaster_id') as broadcaster_id,
    json_extract_scalar(payload, '$.broadcaster_login') as broadcaster_login,
    json_extract_scalar(payload, '$.broadcaster_name') as broadcaster_name,
    json_extract_scalar(payload, '$.title') as title,
    json_extract_scalar(payload, '$.language') as language,
    cast(from_iso8601_timestamp(payload, '$.started_at') as timestamp) as started_at,
    json_extract_scalar(payload, '$.thumbnail_url') as thumbnail_url,
    cast(json_extract_scalar(payload, '$.is_mature') as boolean) as is_mature,
    cast(json_extract_scalar(payload, '$.viewer_count') as integer) as viewer_count,
    cast(json_extract_scalar(payload, '$.pages_fetched') as integer) as pages_fetched,
    cast(json_extract_scalar(payload, '$.is_partial') as boolean) as is_partial,
    cast(json_extract_scalar(payload, '$.dt') as date) as event_date,
    cast(json_extract_scalar(payload, '$.hour') as integer) as event_hour,
    upload_dt as ingested_at,ы
    json_extract_scalar(payload, '$.source_file') as source_file
from {{ ref('raw_steam_twitch_channels') }}
