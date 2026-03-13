from __future__ import annotations

import json
import time
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen

TOKEN_URL = "https://id.twitch.tv/oauth2/token"
SEARCH_CATEGORIES_URL = "https://api.twitch.tv/helix/search/categories"
GAMES_URL = "https://api.twitch.tv/helix/games"
STREAMS_URL = "https://api.twitch.tv/helix/streams"


def http_json(
    url: str,
    timeout: int,
    *,
    headers: dict[str, str] | None = None,
    data: bytes | None = None,
) -> dict[str, Any]:
    req = Request(url, headers=headers or {}, data=data)
    with urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read())


def normalize_name(value: str) -> str:
    return " ".join(value.casefold().split())


def fetch_app_access_token(client_id: str, client_secret: str, timeout: int) -> str:
    payload = urlencode(
        {
            "client_id": client_id,
            "client_secret": client_secret,
            "grant_type": "client_credentials",
        }
    ).encode("utf-8")
    response = http_json(
        TOKEN_URL,
        timeout,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        data=payload,
    )
    token = response.get("access_token")
    if not token:
        raise RuntimeError("Twitch token response did not contain access_token")
    return token


def build_api_headers(client_id: str, access_token: str) -> dict[str, str]:
    return {
        "Client-Id": client_id,
        "Authorization": f"Bearer {access_token}",
        "User-Agent": "codex-twitch-viewers-fetcher",
    }


def search_category(lookup_name: str, timeout: int, headers: dict[str, str]) -> dict[str, Any] | None:
    query = urlencode({"query": lookup_name, "first": 20})
    payload = http_json(f"{SEARCH_CATEGORIES_URL}?{query}", timeout, headers=headers)
    categories = payload.get("data", [])
    if not categories:
        return None

    lookup_name_norm = normalize_name(lookup_name)
    for category in categories:
        category_name = str(category.get("name", ""))
        if normalize_name(category_name) == lookup_name_norm:
            return category
    return None


def get_category_by_id(category_id: str, timeout: int, headers: dict[str, str]) -> dict[str, Any] | None:
    payload = http_json(f"{GAMES_URL}?{urlencode({'id': category_id})}", timeout, headers=headers)
    categories = payload.get("data", [])
    if not categories:
        return None
    return categories[0]


def fetch_category_viewers(
    category_id: str,
    timeout: int,
    headers: dict[str, str],
    max_pages: int | None,
    sleep_seconds: float,
) -> tuple[int, int, int, bool]:
    total_viewers = 0
    total_streams = 0
    pages_fetched = 0
    cursor: str | None = None

    while True:
        params = {"game_id": category_id, "first": 100}
        if cursor:
            params["after"] = cursor
        payload = http_json(f"{STREAMS_URL}?{urlencode(params)}", timeout, headers=headers)
        streams = payload.get("data", [])
        total_viewers += sum(int(stream.get("viewer_count", 0)) for stream in streams)
        total_streams += len(streams)
        pages_fetched += 1

        cursor = payload.get("pagination", {}).get("cursor")
        if not cursor or not streams:
            return total_viewers, total_streams, pages_fetched, False
        if max_pages is not None and pages_fetched >= max_pages:
            return total_viewers, total_streams, pages_fetched, True
        if sleep_seconds > 0:
            time.sleep(sleep_seconds)
