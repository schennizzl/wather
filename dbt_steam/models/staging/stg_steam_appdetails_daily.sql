{% set snapshot_date = var('snapshot_date', none) %}

with source_rows as (
    select
        json_parse(payload) as payload_json,
        cast(source_file as varchar) as source_file,
        cast(replace(substr(ingested_at, 1, 19), 'T', ' ') as timestamp(3)) as ingested_at,
        cast(dt as date) as snapshot_date
    from {{ source('raw', 'appdetails_daily_files') }}
    where payload is not null
),
target_snapshot as (
    select
        {% if snapshot_date is not none %}
        max(cast('{{ snapshot_date }}' as date)) as snapshot_date
        {% else %}
        max(snapshot_date) as snapshot_date
        {% endif %}
    from source_rows
),
latest_snapshot_file as (
    select
        source_file,
        ingested_at,
        row_number() over (
            order by ingested_at desc, source_file desc
        ) as rn
    from source_rows
    where snapshot_date = (select snapshot_date from target_snapshot)
    group by source_file, ingested_at
)
select
    cast(json_extract_scalar(payload_json, '$.appid') as bigint) as appid,
    json_extract_scalar(payload_json, '$.name') as app_name,
    json_extract_scalar(payload_json, '$.type') as app_type,
    cast(json_extract_scalar(payload_json, '$.is_free') as boolean) as is_free,
    cast(json_extract_scalar(payload_json, '$.required_age') as integer) as required_age,
    json_extract_scalar(payload_json, '$.short_description') as short_description,
    json_extract_scalar(payload_json, '$.about_the_game') as about_the_game,
    json_extract_scalar(payload_json, '$.supported_languages') as supported_languages,
    json_extract_scalar(payload_json, '$.developers') as developers_json,
    json_extract_scalar(payload_json, '$.publishers') as publishers_json,
    json_extract_scalar(payload_json, '$.website') as website,
    cast(json_extract_scalar(payload_json, '$.platform_windows') as boolean) as platform_windows,
    cast(json_extract_scalar(payload_json, '$.platform_mac') as boolean) as platform_mac,
    cast(json_extract_scalar(payload_json, '$.platform_linux') as boolean) as platform_linux,
    cast(json_extract_scalar(payload_json, '$.metacritic_score') as integer) as metacritic_score,
    cast(json_extract_scalar(payload_json, '$.recommendations_total') as integer) as recommendations_total,
    json_extract_scalar(payload_json, '$.release_date') as release_date_text,
    cast(json_extract_scalar(payload_json, '$.coming_soon') as boolean) as coming_soon,
    json_extract_scalar(payload_json, '$.price_currency') as price_currency,
    cast(json_extract_scalar(payload_json, '$.price_initial') as bigint) as price_initial,
    cast(json_extract_scalar(payload_json, '$.price_final') as bigint) as price_final,
    json_extract_scalar(payload_json, '$.categories_json') as categories_json,
    json_extract_scalar(payload_json, '$.genres_json') as genres_json,
    s.snapshot_date,
    s.ingested_at,
    s.source_file
from source_rows s
join latest_snapshot_file f
    on s.source_file = f.source_file
   and s.ingested_at = f.ingested_at
where s.snapshot_date = (select snapshot_date from target_snapshot)
  and f.rn = 1
