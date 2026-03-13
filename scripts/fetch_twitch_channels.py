from __future__ import annotations

import argparse
import time
from pathlib import Path
from urllib.parse import urlencode

from game_helpers import load_games_with_twitch
from io_helpers import build_metadata_fields, write_enveloped_ndjson
from twitch_helpers import (
    STREAMS_URL,
    build_api_headers,
    fetch_app_access_token,
    get_category_by_id,
    http_json,
    search_category,
)


def fetch_category_channels(
    category_id: str,
    timeout: int,
    headers: dict[str, str],
    max_pages: int | None,
    sleep_seconds: float,
) -> tuple[list[dict[str, object]], int, bool]:
    streams: list[dict[str, object]] = []
    seen_user_ids: set[str] = set()
    pages_fetched = 0
    cursor: str | None = None

    while True:
        params = {"game_id": category_id, "first": 100}
        if cursor:
            params["after"] = cursor
        payload = http_json(f"{STREAMS_URL}?{urlencode(params)}", timeout, headers=headers)
        page_streams = payload.get("data", [])
        for stream in page_streams:
            user_id = stream.get("user_id")
            if user_id is None:
                streams.append(stream)
                continue
            user_id_str = str(user_id)
            if user_id_str in seen_user_ids:
                continue
            seen_user_ids.add(user_id_str)
            streams.append(stream)
        pages_fetched += 1

        cursor = payload.get("pagination", {}).get("cursor")
        if not cursor or not page_streams:
            return streams, pages_fetched, False
        if max_pages is not None and pages_fetched >= max_pages:
            return streams, pages_fetched, True
        if sleep_seconds > 0:
            time.sleep(sleep_seconds)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch Twitch live channels for a fixed game list.")
    parser.add_argument("--games-file", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--client-id", type=str, required=True)
    parser.add_argument("--client-secret", type=str, required=True)
    parser.add_argument("--timeout", type=int, default=30)
    parser.add_argument("--sleep", type=float, default=0.0)
    parser.add_argument("--max-pages", type=int, default=0)
    parser.add_argument("--meta-source-file", type=str, default=None)
    parser.add_argument("--meta-ingested-at", type=str, default=None)
    parser.add_argument("--meta-dt", type=str, default=None)
    parser.add_argument("--meta-hour", type=str, default=None)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    metadata_fields = build_metadata_fields(args)

    if not args.client_id.strip() or not args.client_secret.strip():
        raise RuntimeError("Twitch credentials are required via TWITCH_CLIENT_ID and TWITCH_CLIENT_SECRET")

    access_token = fetch_app_access_token(args.client_id, args.client_secret, args.timeout)
    headers = build_api_headers(args.client_id, access_token)
    max_pages = args.max_pages if args.max_pages and args.max_pages > 0 else None

    records: list[dict[str, object]] = []
    for appid, game_name, twitch_lookup_name, configured_twitch_category_id in load_games_with_twitch(args.games_file):
        category = None
        if configured_twitch_category_id is not None:
            category = get_category_by_id(
                category_id=configured_twitch_category_id,
                timeout=args.timeout,
                headers=headers,
            )
        if category is None:
            category = search_category(
                lookup_name=twitch_lookup_name,
                timeout=args.timeout,
                headers=headers,
            )

        if category is None:
            records.append(
                {
                    "appid": appid,
                    "game_name": game_name,
                    "twitch_lookup_name": twitch_lookup_name,
                    "configured_twitch_category_id": configured_twitch_category_id,
                    "twitch_category_id": None,
                    "twitch_category_name": None,
                    "broadcaster_id": None,
                    "broadcaster_login": None,
                    "broadcaster_name": None,
                    "title": None,
                    "language": None,
                    "started_at": None,
                    "thumbnail_url": None,
                    "is_mature": None,
                    "viewer_count": None,
                    "pages_fetched": 0,
                    "is_partial": False,
                }
            )
            continue

        category_id = str(category["id"])
        category_name = category.get("name")
        streams, pages_fetched, is_partial = fetch_category_channels(
            category_id=category_id,
            timeout=args.timeout,
            headers=headers,
            max_pages=max_pages,
            sleep_seconds=args.sleep,
        )

        if not streams:
            records.append(
                {
                    "appid": appid,
                    "game_name": game_name,
                    "twitch_lookup_name": twitch_lookup_name,
                    "configured_twitch_category_id": configured_twitch_category_id,
                    "twitch_category_id": category_id,
                    "twitch_category_name": category_name,
                    "broadcaster_id": None,
                    "broadcaster_login": None,
                    "broadcaster_name": None,
                    "title": None,
                    "language": None,
                    "started_at": None,
                    "thumbnail_url": None,
                    "is_mature": None,
                    "viewer_count": 0,
                    "pages_fetched": pages_fetched,
                    "is_partial": is_partial,
                }
            )
            continue

        for stream in streams:
            records.append(
                {
                    "appid": appid,
                    "game_name": game_name,
                    "twitch_lookup_name": twitch_lookup_name,
                    "configured_twitch_category_id": configured_twitch_category_id,
                    "twitch_category_id": category_id,
                    "twitch_category_name": category_name,
                    "broadcaster_id": stream.get("user_id"),
                    "broadcaster_login": stream.get("user_login"),
                    "broadcaster_name": stream.get("user_name"),
                    "title": stream.get("title"),
                    "language": stream.get("language"),
                    "started_at": stream.get("started_at"),
                    "thumbnail_url": stream.get("thumbnail_url"),
                    "is_mature": stream.get("is_mature"),
                    "viewer_count": stream.get("viewer_count"),
                    "pages_fetched": pages_fetched,
                    "is_partial": is_partial,
                }
            )

        if args.sleep > 0:
            time.sleep(args.sleep)

    write_enveloped_ndjson(records, args.output, metadata_fields)
    print(f"Wrote {len(records)} twitch channel rows to {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
