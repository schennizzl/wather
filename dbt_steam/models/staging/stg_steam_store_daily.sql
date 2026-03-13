{% set snapshot_date = var('snapshot_date', none) %}

with source_rows as (
    select
        json_parse(payload) as payload_json,
        cast(source_file as varchar) as source_file,
        cast(replace(substr(ingested_at, 1, 19), 'T', ' ') as timestamp(3)) as ingested_at,
        cast(dt as date) as snapshot_date
    from {{ source('raw', 'store_daily_files') }}
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
    s.snapshot_date,
    s.ingested_at,
    s.source_file
from source_rows s
join latest_snapshot_file f
    on s.source_file = f.source_file
   and s.ingested_at = f.ingested_at
where s.snapshot_date = (select snapshot_date from target_snapshot)
  and f.rn = 1
