{{ config(
    materialized='incremental',
    incremental_strategy='append',
    on_schema_change='sync_all_columns'
) }}

with candidate_source as (
    select
        payload,
        dt,
        hour,
        ingested_at,
        source_file
    from {{ source('raw', 'twitch_channels_files') }}
    where payload is not null
    {% if is_incremental() %}
        -- Re-read only the latest partition day plus one-day overlap for safety.
        and cast(dt as date) >= coalesce(
            (
                select date_add('day', -1, max(event_date))
                from {{ this }}
            ),
            date '1970-01-01'
        )
    {% endif %}
),

source_rows as (
    select
        cast(json_extract_scalar(json_parse(payload), '$.appid') as bigint) as appid,
        json_extract_scalar(json_parse(payload), '$.game_name') as game_name,
        json_extract_scalar(json_parse(payload), '$.twitch_lookup_name') as twitch_lookup_name,
        json_extract_scalar(json_parse(payload), '$.configured_twitch_category_id') as configured_twitch_category_id,
        json_extract_scalar(json_parse(payload), '$.twitch_category_id') as twitch_category_id,
        json_extract_scalar(json_parse(payload), '$.twitch_category_name') as twitch_category_name,
        json_extract_scalar(json_parse(payload), '$.broadcaster_id') as broadcaster_id,
        json_extract_scalar(json_parse(payload), '$.broadcaster_login') as broadcaster_login,
        json_extract_scalar(json_parse(payload), '$.broadcaster_name') as broadcaster_name,
        json_extract_scalar(json_parse(payload), '$.title') as title,
        json_extract_scalar(json_parse(payload), '$.language') as language,
        cast(from_iso8601_timestamp(json_extract_scalar(json_parse(payload), '$.started_at')) as timestamp) as started_at,
        json_extract_scalar(json_parse(payload), '$.thumbnail_url') as thumbnail_url,
        cast(json_extract_scalar(json_parse(payload), '$.is_mature') as boolean) as is_mature,
        cast(json_extract_scalar(json_parse(payload), '$.viewer_count') as integer) as viewer_count,
        cast(json_extract_scalar(json_parse(payload), '$.pages_fetched') as integer) as pages_fetched,
        cast(json_extract_scalar(json_parse(payload), '$.is_partial') as boolean) as is_partial,
        cast(dt as date) as event_date,
        cast(hour as integer) as event_hour,
        cast(replace(substr(ingested_at, 1, 19), 'T', ' ') as timestamp(3)) as ingested_at,
        cast(source_file as varchar) as source_file
    from candidate_source
)

select src.*
from source_rows as src
{% if is_incremental() %}
where not exists (
    select 1
    from {{ this }} as tgt
    where tgt.source_file = src.source_file
)
{% endif %}
