select
    cast(json_extract_scalar(json_parse(payload), '$.appid') as bigint) as appid,
    json_extract_scalar(json_parse(payload), '$.game_name') as game_name,
    cast(json_extract_scalar(json_parse(payload), '$.current_players') as integer) as current_players,
    cast(dt as date) as event_date,
    cast(hour as integer) as event_hour,
    cast(replace(substr(ingested_at, 1, 19), 'T', ' ') as timestamp(3)) as ingested_at,
    cast(source_file as varchar) as source_file
from {{ source('raw', 'game_online_files') }}
where payload is not null
