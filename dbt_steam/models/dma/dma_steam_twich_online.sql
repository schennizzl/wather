{{ config(
    materialized='incremental',
    incremental_strategy='append'
) }}

with steam_online as (
	select
		steam_online.appid as steam_app_id,
		game_name.game_name as steam_game_name,
		steam_online.current_players as current_steam_players,
		steam_online.event_ts_hour
	from {{ ref('ods_fact_game_online_hourly') }} as steam_online
	left join {{ ref('ods_game_name_history') }} as game_name
		on steam_online.appid = game_name.appid
),
twitch_online as (
	select
		twitch_online.appid,
		twitch_online.twitch_category_id,
		twitch_name.twitch_category_name,
		twitch_online.approx_total_viewers,
		twitch_online.live_channels,
		twitch_online.event_ts_hour
	from {{ ref('ods_fact_twitch_category_hourly') }} as twitch_online
	left join {{ ref('ods_twitch_category_name_history') }} as twitch_name
		on twitch_name.twitch_category_id = twitch_online.twitch_category_id
		and twitch_name.valid_to_ts is NULL
),
streamers as (
	select
		streamers.appid,
		streamers.broadcaster_id,
		profile.broadcaster_login,
		profile.broadcaster_name,
		streamers.language,
		streamers.viewer_count,
		streamers.event_ts_hour
	from {{ ref('ods_fact_twitch_stream_hourly') }} as streamers
	left join {{ ref('ods_twitch_broadcaster_profile_history') }} as profile
		on streamers.broadcaster_id = profile.broadcaster_id
		and profile.valid_to_ts is null
)
select
    to_hex(
        sha256(
            to_utf8(
                concat_ws(
                    '',
                    cast(steam_online.steam_app_id as varchar),
                    cast(twitch_online.twitch_category_id as varchar),
                    cast(streamers.broadcaster_id as varchar),
                    cast(steam_online.event_ts_hour as varchar)
                )
            )
        )
    ) as sk_steam_twich_hex,
	steam_online.steam_app_id,
	twitch_online.twitch_category_id,
	steam_online.steam_game_name,
	twitch_online.twitch_category_name,
	steam_online.current_steam_players,
	twitch_online.approx_total_viewers,
	twitch_online.live_channels,
	streamers.broadcaster_id,
	streamers.broadcaster_login,
	streamers.broadcaster_name,
	streamers.language,
	streamers.viewer_count,
	steam_online.event_ts_hour
from steam_online
left join twitch_online
	on steam_online.steam_app_id = twitch_online.appid
	and steam_online.event_ts_hour = twitch_online.event_ts_hour
left join streamers
	on steam_app_id = streamers.appid
	and steam_online.event_ts_hour = streamers.event_ts_hour
order by
	steam_online.steam_app_id,
	event_ts_hour