with category_rows as (
    select twitch_category_id, ingested_at
    from {{ ref('stg_steam_twitch_viewers') }}
    where twitch_category_id is not null

    union all

    select twitch_category_id, ingested_at
    from {{ ref('stg_steam_twitch_channels') }}
    where twitch_category_id is not null
)
select
    twitch_category_id,
    min(ingested_at) as first_seen_at,
    max(ingested_at) as last_seen_at
from category_rows
group by twitch_category_id
