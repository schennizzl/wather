with category_name_rows as (
    select twitch_category_id, twitch_category_name, ingested_at
    from {{ ref('stg_steam_twitch_viewers') }}
    where twitch_category_id is not null and twitch_category_name is not null

    union all

    select twitch_category_id, twitch_category_name, ingested_at
    from {{ ref('stg_steam_twitch_channels') }}
    where twitch_category_id is not null and twitch_category_name is not null
),
name_ranges as (
    select
        twitch_category_id,
        twitch_category_name,
        min(ingested_at) as valid_from_ts,
        max(ingested_at) as last_seen_ts
    from category_name_rows
    group by twitch_category_id, twitch_category_name
)
select
    twitch_category_id,
    twitch_category_name,
    valid_from_ts,
    lead(valid_from_ts) over (partition by twitch_category_id order by valid_from_ts, twitch_category_name) as valid_to_ts,
    last_seen_ts
from name_ranges
