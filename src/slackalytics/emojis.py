from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

import duckdb
import emoji


SLACK_EMOJI_LIST_URL = "https://slack.com/api/emoji.list"
SKIN_TONE_SUFFIXES = {
    "skin-tone-2": "\U0001F3FB",
    "skin-tone-3": "\U0001F3FC",
    "skin-tone-4": "\U0001F3FD",
    "skin-tone-5": "\U0001F3FE",
    "skin-tone-6": "\U0001F3FF",
}


@dataclass(frozen=True)
class EmojiLookupRow:
    emoji_name: str
    emoji_kind: str
    display_value: str
    display_name: str
    unicode_glyph: str | None
    image_url: str | None
    alias_target: str | None
    resolved_name: str | None
    is_custom: bool


class SlackEmojiSyncError(RuntimeError):
    pass


def fetch_slack_emoji_catalog(token: str) -> dict[str, str]:
    request = Request(
        SLACK_EMOJI_LIST_URL,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/x-www-form-urlencoded",
        },
        method="GET",
    )
    try:
        with urlopen(request) as response:
            payload = json.load(response)
    except HTTPError as exc:
        raise SlackEmojiSyncError(f"Slack API request failed with HTTP {exc.code}") from exc
    except URLError as exc:
        raise SlackEmojiSyncError(f"Slack API request failed: {exc.reason}") from exc

    if not payload.get("ok"):
        error = payload.get("error", "unknown_error")
        raise SlackEmojiSyncError(f"Slack emoji.list failed: {error}")

    emoji_map = payload.get("emoji")
    if not isinstance(emoji_map, dict):
        raise SlackEmojiSyncError("Slack emoji.list response did not include an emoji map")
    return {str(name): str(value) for name, value in emoji_map.items()}


def sync_emojis(
    conn: duckdb.DuckDBPyConnection,
    token: str | None = None,
) -> tuple[int, int]:
    emoji_catalog = fetch_slack_emoji_catalog(token) if token else {}
    reaction_names = {
        row[0]
        for row in conn.execute(
            "select distinct reaction_name from raw.message_reactions where reaction_name is not null"
        ).fetchall()
    }
    lookup_rows = build_emoji_lookup_rows(reaction_names=reaction_names, emoji_catalog=emoji_catalog)
    synced_at = datetime.now(UTC)

    conn.execute("begin transaction")
    try:
        conn.execute("delete from raw.emoji_catalog")
        if emoji_catalog:
            conn.executemany(
                """
                insert into raw.emoji_catalog (emoji_name, raw_value, synced_at)
                values (?, ?, ?)
                """,
                [(name, value, synced_at) for name, value in sorted(emoji_catalog.items())],
            )

        conn.execute("delete from analytics.emoji_lookup")
        if lookup_rows:
            conn.executemany(
                """
                insert into analytics.emoji_lookup (
                    emoji_name,
                    emoji_kind,
                    display_value,
                    display_name,
                    unicode_glyph,
                    image_url,
                    alias_target,
                    resolved_name,
                    is_custom,
                    synced_at
                )
                values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        row.emoji_name,
                        row.emoji_kind,
                        row.display_value,
                        row.display_name,
                        row.unicode_glyph,
                        row.image_url,
                        row.alias_target,
                        row.resolved_name,
                        row.is_custom,
                        synced_at,
                    )
                    for row in lookup_rows
                ],
            )
        conn.execute("commit")
    except Exception:
        conn.execute("rollback")
        raise

    return len(emoji_catalog), len(lookup_rows)


def build_emoji_lookup_rows(
    *,
    reaction_names: set[str],
    emoji_catalog: dict[str, str],
) -> list[EmojiLookupRow]:
    all_names = sorted(set(reaction_names) | set(emoji_catalog))
    return [resolve_emoji_name(name, emoji_catalog) for name in all_names]


def resolve_emoji_name(name: str, emoji_catalog: dict[str, str]) -> EmojiLookupRow:
    visited: set[str] = set()
    cursor = name
    alias_target: str | None = None

    while True:
        if cursor in visited:
            return fallback_row(
                name=name,
                kind="alias_unresolved",
                alias_target=alias_target or cursor,
                resolved_name=cursor,
                is_custom=True,
            )
        visited.add(cursor)

        standard = standard_emoji_for_name(cursor)
        if standard is not None:
            return EmojiLookupRow(
                emoji_name=name,
                emoji_kind="unicode",
                display_value=f"{standard} {name}",
                display_name=name,
                unicode_glyph=standard,
                image_url=None,
                alias_target=alias_target,
                resolved_name=cursor,
                is_custom=name in emoji_catalog and emoji_catalog.get(name, "").startswith("alias:"),
            )

        raw_value = emoji_catalog.get(cursor)
        if raw_value is None:
            return fallback_row(
                name=name,
                kind="unknown",
                alias_target=alias_target,
                resolved_name=cursor if cursor != name else None,
                is_custom=name in emoji_catalog,
            )

        if raw_value.startswith("alias:"):
            alias_target = raw_value.removeprefix("alias:")
            cursor = alias_target
            continue

        if raw_value.startswith("http://") or raw_value.startswith("https://"):
            display_name = alias_target or name
            return EmojiLookupRow(
                emoji_name=name,
                emoji_kind="custom_image",
                display_value=f":{display_name}: {display_name}",
                display_name=display_name,
                unicode_glyph=None,
                image_url=raw_value,
                alias_target=alias_target,
                resolved_name=cursor,
                is_custom=True,
            )

        return fallback_row(
            name=name,
            kind="unknown",
            alias_target=alias_target,
            resolved_name=cursor,
            is_custom=True,
        )


def standard_emoji_for_name(name: str) -> str | None:
    base_name, skin_tone_suffix = split_skin_tone(name)
    alias = f":{base_name}:"
    rendered = emoji.emojize(alias, language="alias")
    if rendered == alias:
        return None
    if skin_tone_suffix is not None:
        modifier = SKIN_TONE_SUFFIXES.get(skin_tone_suffix)
        if modifier is not None:
            rendered = f"{rendered}{modifier}"
    return rendered


def split_skin_tone(name: str) -> tuple[str, str | None]:
    if "::" not in name:
        return name, None
    base_name, suffix = name.split("::", 1)
    if suffix not in SKIN_TONE_SUFFIXES:
        return name, None
    return base_name, suffix


def fallback_row(
    *,
    name: str,
    kind: str,
    alias_target: str | None,
    resolved_name: str | None,
    is_custom: bool,
) -> EmojiLookupRow:
    return EmojiLookupRow(
        emoji_name=name,
        emoji_kind=kind,
        display_value=f":{name}: {name}",
        display_name=name,
        unicode_glyph=None,
        image_url=None,
        alias_target=alias_target,
        resolved_name=resolved_name,
        is_custom=is_custom,
    )


def row_dicts(rows: list[EmojiLookupRow]) -> list[dict[str, Any]]:
    return [asdict(row) for row in rows]
