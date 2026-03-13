from __future__ import annotations

from pathlib import Path


def load_games_basic(path: Path) -> list[tuple[int, str]]:
    games: list[tuple[int, str]] = []
    with path.open("r", encoding="utf-8") as handle:
        for raw_line in handle:
            line = raw_line.strip()
            if not line:
                continue
            parts = line.split("\t")
            if len(parts) < 2:
                raise ValueError(f"Expected at least 2 tab-separated columns in {path}: {line!r}")
            appid_raw, game_name = parts[0], parts[1]
            games.append((int(appid_raw), game_name))
    return games


def load_games_with_twitch(path: Path) -> list[tuple[int, str, str, str | None]]:
    games: list[tuple[int, str, str, str | None]] = []
    with path.open("r", encoding="utf-8") as handle:
        for raw_line in handle:
            line = raw_line.strip()
            if not line:
                continue
            parts = line.split("\t")
            if len(parts) < 2:
                raise ValueError(f"Expected at least 2 tab-separated columns in {path}: {line!r}")
            appid_raw, game_name = parts[0], parts[1]
            twitch_lookup_name = parts[2] if len(parts) >= 3 and parts[2] else game_name
            twitch_category_id = parts[3] if len(parts) >= 4 and parts[3] else None
            games.append((int(appid_raw), game_name, twitch_lookup_name, twitch_category_id))
    return games
