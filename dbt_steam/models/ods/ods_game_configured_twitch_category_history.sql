with configured_rows as (
    select
        appid,
        configured_twitch_category_id,
        ingested_at
    from {{ ref('stg_steam_twitch_channels') }}
    where configured_twitch_category_id is not null
),
configured_ranges as (
    select
        appid,
        configured_twitch_category_id,
        min(ingested_at) as valid_from_ts,
        max(ingested_at) as last_seen_ts
    from configured_rows
    group by appid, configured_twitch_category_id
)
select
    appid,
    configured_twitch_category_id,
    valid_from_ts,
    lead(valid_from_ts) over (partition by appid order by valid_from_ts, configured_twitch_category_id) as valid_to_ts,
    last_seen_ts
from configured_ranges
