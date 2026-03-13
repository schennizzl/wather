from __future__ import annotations

import argparse
import time
from pathlib import Path
from io_helpers import build_metadata_fields, write_enveloped_ndjson
from game_helpers import load_games_with_twitch
from twitch_helpers import (
    build_api_headers,
    fetch_app_access_token,
    fetch_category_viewers,
    get_category_by_id,
    search_category,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch Twitch live viewers for a fixed game list.")
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
    for appid, game_name, twitch_lookup_name, twitch_category_id in load_games_with_twitch(args.games_file):
        category = None
        if twitch_category_id is not None:
            category = get_category_by_id(
                category_id=twitch_category_id,
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
                    "configured_twitch_category_id": twitch_category_id,
                    "twitch_category_id": None,
                    "twitch_category_name": None,
                    "approx_total_viewers": None,
                    "live_channels": 0,
                    "pages_fetched": 0,
                    "is_partial": False,
                }
            )
            continue

        viewers, streams, pages_fetched, is_partial = fetch_category_viewers(
            category_id=str(category["id"]),
            timeout=args.timeout,
            headers=headers,
            max_pages=max_pages,
            sleep_seconds=args.sleep,
        )
        records.append(
            {
                "appid": appid,
                "game_name": game_name,
                "twitch_lookup_name": twitch_lookup_name,
                "configured_twitch_category_id": twitch_category_id,
                "twitch_category_id": str(category["id"]),
                "twitch_category_name": category.get("name"),
                "approx_total_viewers": viewers,
                "live_channels": streams,
                "pages_fetched": pages_fetched,
                "is_partial": is_partial,
            }
        )
        if args.sleep > 0:
            time.sleep(args.sleep)

    write_enveloped_ndjson(records, args.output, metadata_fields)
    print(f"Wrote {len(records)} twitch viewer rows to {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
