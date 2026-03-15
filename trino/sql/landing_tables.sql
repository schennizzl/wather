CREATE SCHEMA IF NOT EXISTS hive.raw
WITH (location = 's3a://raw/steam/raw/');

CREATE SCHEMA IF NOT EXISTS hive.stg
WITH (location = 's3a://dwh/warehouse/stg/');

CREATE TABLE IF NOT EXISTS hive.raw.store_daily_files (
    payload VARCHAR,
    source_file VARCHAR,
    ingested_at VARCHAR,
    dt DATE
)
WITH (
    external_location = 's3a://raw/steam/raw/store_daily/',
    format = 'JSON',
    partitioned_by = ARRAY['dt']
);

CREATE TABLE IF NOT EXISTS hive.raw.appdetails_types_files (
    payload VARCHAR,
    source_file VARCHAR,
    ingested_at VARCHAR,
    dt DATE,
    hour INTEGER
)
WITH (
    external_location = 's3a://raw/steam/raw/appdetails_types/',
    format = 'JSON',
    partitioned_by = ARRAY['dt', 'hour']
);

CREATE TABLE IF NOT EXISTS hive.raw.appdetails_daily_files (
    payload VARCHAR,
    source_file VARCHAR,
    ingested_at VARCHAR,
    dt DATE
)
WITH (
    external_location = 's3a://raw/steam/raw/appdetails_daily/',
    format = 'JSON',
    partitioned_by = ARRAY['dt']
);

CREATE TABLE IF NOT EXISTS hive.raw.game_online_files (
    payload VARCHAR,
    source_file VARCHAR,
    ingested_at VARCHAR,
    dt DATE,
    hour INTEGER
)
WITH (
    external_location = 's3a://raw/steam/raw/game_online/',
    format = 'JSON',
    partitioned_by = ARRAY['dt', 'hour']
);

CREATE TABLE IF NOT EXISTS hive.raw.twitch_viewers_files (
    payload VARCHAR,
    source_file VARCHAR,
    ingested_at VARCHAR,
    dt DATE,
    hour INTEGER
)
WITH (
    external_location = 's3a://raw/steam/raw/twitch_viewers/',
    format = 'JSON',
    partitioned_by = ARRAY['dt', 'hour']
);

CREATE TABLE IF NOT EXISTS hive.raw.twitch_channels_files (
    payload VARCHAR,
    source_file VARCHAR,
    ingested_at VARCHAR,
    dt DATE,
    hour INTEGER
)
WITH (
    external_location = 's3a://raw/steam/raw/twitch_channels/',
    format = 'JSON',
    partitioned_by = ARRAY['dt', 'hour']
);
