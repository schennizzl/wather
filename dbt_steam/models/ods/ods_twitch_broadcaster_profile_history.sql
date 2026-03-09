with profile_rows as (
    select
        broadcaster_id,
        broadcaster_login,
        broadcaster_name,
        ingested_at
    from {{ ref('stg_steam_twitch_channels') }}
    where broadcaster_id is not null
      and broadcaster_login is not null
      and broadcaster_name is not null
),
profile_ranges as (
    select
        broadcaster_id,
        broadcaster_login,
        broadcaster_name,
        min(ingested_at) as valid_from_ts,
        max(ingested_at) as last_seen_ts
    from profile_rows
    group by broadcaster_id, broadcaster_login, broadcaster_name
)
select
    broadcaster_id,
    broadcaster_login,
    broadcaster_name,
    valid_from_ts,
    lead(valid_from_ts) over (partition by broadcaster_id order by valid_from_ts, broadcaster_login, broadcaster_name) as valid_to_ts,
    last_seen_ts
from profile_ranges
