from __future__ import annotations

import argparse
import json
import time
from pathlib import Path
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from game_helpers import load_games_basic
from io_helpers import build_metadata_fields, write_enveloped_ndjson

API_URL_CURRENT_PLAYERS = "https://api.steampowered.com/ISteamUserStats/GetNumberOfCurrentPlayers/v1/"


def _get_json(url: str, timeout: int) -> dict[str, Any]:
    req = Request(url, headers={"User-Agent": "codex-game-online-fetcher"})
    with urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read())


def fetch_current_players(appid: int, timeout: int) -> int | None:
    query = urlencode({"appid": appid})
    payload = _get_json(f"{API_URL_CURRENT_PLAYERS}?{query}", timeout)
    response = payload.get("response", {})
    if response.get("result") != 1:
        return None
    return response.get("player_count")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch current Steam players for a fixed game list.")
    parser.add_argument("--games-file", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--timeout", type=int, default=30)
    parser.add_argument("--sleep", type=float, default=0.0)
    parser.add_argument("--meta-source-file", type=str, default=None)
    parser.add_argument("--meta-ingested-at", type=str, default=None)
    parser.add_argument("--meta-dt", type=str, default=None)
    parser.add_argument("--meta-hour", type=str, default=None)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    metadata_fields = build_metadata_fields(args)

    records: list[dict[str, Any]] = []
    for appid, game_name in load_games_basic(args.games_file):
        records.append(
            {
                "appid": appid,
                "game_name": game_name,
                "current_players": fetch_current_players(appid=appid, timeout=args.timeout),
            }
        )
        if args.sleep > 0:
            time.sleep(args.sleep)

    write_enveloped_ndjson(records, args.output, metadata_fields)
    print(f"Wrote {len(records)} online rows to {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
