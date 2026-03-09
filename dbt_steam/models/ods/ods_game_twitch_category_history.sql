with category_rows as (
    select appid, twitch_category_id, ingested_at
    from {{ ref('stg_steam_twitch_viewers') }}
    where twitch_category_id is not null

    union all

    select appid, twitch_category_id, ingested_at
    from {{ ref('stg_steam_twitch_channels') }}
    where twitch_category_id is not null
),
category_ranges as (
    select
        appid,
        twitch_category_id,
        min(ingested_at) as valid_from_ts,
        max(ingested_at) as last_seen_ts
    from category_rows
    group by appid, twitch_category_id
)
select
    appid,
    twitch_category_id,
    valid_from_ts,
    lead(valid_from_ts) over (partition by appid order by valid_from_ts, twitch_category_id) as valid_to_ts,
    last_seen_ts
from category_ranges
