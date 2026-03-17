from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from time import monotonic
from typing import Any, Callable
from zipfile import ZipFile

import duckdb
import pandas as pd

from slackalytics.db import ensure_schema


EXPORT_DATE_PATTERN = re.compile(r"- (?P<month>[A-Za-z]{3}) (?P<day>\d{1,2}) (?P<year>\d{4})\.zip$")


@dataclass(frozen=True)
class SourceMetadata:
    source_id: str
    source_name: str
    source_path: str
    file_hash: str
    source_size_bytes: int
    source_modified_at: datetime
    exported_through: datetime | None


@dataclass(frozen=True)
class ParseStats:
    channel_files: int
    messages: int
    reactions: int


def discover_zip_sources(exports_dir: Path) -> list[Path]:
    return sorted(path for path in exports_dir.iterdir() if path.suffix == ".zip")


def build_source_metadata(path: Path, reporter: Reporter | None = None) -> SourceMetadata:
    stat = path.stat()
    emit(reporter, f"[hash] {path.name} ({format_bytes(stat.st_size)})")
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    source_hash = digest.hexdigest()
    exported_through = parse_exported_through(path.name)
    return SourceMetadata(
        source_id=source_hash,
        source_name=path.name,
        source_path=str(path.resolve()),
        file_hash=source_hash,
        source_size_bytes=stat.st_size,
        source_modified_at=datetime.fromtimestamp(stat.st_mtime, tz=UTC),
        exported_through=exported_through,
    )


Reporter = Callable[[str], None]


def parse_exported_through(filename: str) -> datetime | None:
    match = EXPORT_DATE_PATTERN.search(filename)
    if not match:
        return None
    date_value = f"{match.group('month')} {match.group('day')} {match.group('year')}"
    return datetime.strptime(date_value, "%b %d %Y").replace(tzinfo=UTC)


def ingest_exports(
    conn: duckdb.DuckDBPyConnection,
    exports_dir: Path,
    reporter: Reporter | None = None,
) -> list[SourceMetadata]:
    ensure_schema(conn)
    source_paths = discover_zip_sources(exports_dir)
    emit(reporter, f"[discover] found {len(source_paths)} export zip(s) in {exports_dir}")
    sources = [build_source_metadata(path, reporter=reporter) for path in source_paths]
    for index, source in enumerate(sources, start=1):
        emit(
            reporter,
            f"[source {index}/{len(sources)}] {source.source_name} through "
            f"{source.exported_through.date() if source.exported_through else 'unknown'}",
        )
        ingest_source(conn, Path(source.source_path), source, reporter=reporter)
    return sources


def ingest_source(
    conn: duckdb.DuckDBPyConnection,
    source_path: Path,
    metadata: SourceMetadata,
    reporter: Reporter | None = None,
) -> None:
    ensure_schema(conn)
    started_at = monotonic()
    emit(reporter, f"[open] {metadata.source_name}")
    with ZipFile(source_path) as zf:
        users = json.loads(zf.read("users.json"))
        channels = json.loads(zf.read("channels.json"))
        messages, reactions, stats = parse_messages(zf, metadata.source_id, reporter=reporter)

    emit(
        reporter,
        f"[parsed] users={len(users)} channels={len(channels)} channel_files={stats.channel_files} "
        f"messages={stats.messages} reactions={stats.reactions} elapsed={format_elapsed(monotonic() - started_at)}",
    )

    emit(reporter, "[load] registering incoming dataframes")
    conn.register(
        "incoming_ingest_sources",
        pd.DataFrame(
            [
                {
                    "source_id": metadata.source_id,
                    "source_name": metadata.source_name,
                    "source_path": metadata.source_path,
                    "file_hash": metadata.file_hash,
                    "source_size_bytes": metadata.source_size_bytes,
                    "source_modified_at": metadata.source_modified_at,
                    "exported_through": metadata.exported_through,
                }
            ]
        ),
    )
    conn.register(
        "incoming_users",
        pd.DataFrame([normalize_user(user, metadata.source_id) for user in users]),
    )
    conn.register(
        "incoming_channels",
        pd.DataFrame([normalize_channel(channel, metadata.source_id) for channel in channels]),
    )
    conn.register("incoming_messages", pd.DataFrame(messages))
    conn.register("incoming_message_reactions", pd.DataFrame(reactions))

    emit(reporter, "[merge] ingest_sources")
    conn.execute(
        """
        merge into raw.ingest_sources target
        using incoming_ingest_sources source
        on target.source_id = source.source_id
        when matched then update set
            source_name = source.source_name,
            source_path = source.source_path,
            file_hash = source.file_hash,
            source_size_bytes = source.source_size_bytes,
            source_modified_at = source.source_modified_at,
            exported_through = source.exported_through,
            ingested_at = current_timestamp
        when not matched then insert (
            source_id,
            source_name,
            source_path,
            file_hash,
            source_size_bytes,
            source_modified_at,
            exported_through
        ) values (
            source.source_id,
            source.source_name,
            source.source_path,
            source.file_hash,
            source.source_size_bytes,
            source.source_modified_at,
            source.exported_through
        )
        """
    )
    emit(reporter, "[merge] users")
    conn.execute(
        """
        merge into raw.users target
        using incoming_users source
        on target.user_id = source.user_id
        when matched then update set
            name = source.name,
            real_name = source.real_name,
            is_bot = source.is_bot,
            is_deleted = source.is_deleted,
            tz = source.tz,
            updated_at = source.updated_at,
            profile = source.profile,
            source_id = source.source_id,
            last_seen_at = current_timestamp
        when not matched then insert (
            user_id, name, real_name, is_bot, is_deleted, tz, updated_at, profile, source_id
        ) values (
            source.user_id, source.name, source.real_name, source.is_bot, source.is_deleted,
            source.tz, source.updated_at, source.profile, source.source_id
        )
        """
    )
    emit(reporter, "[merge] channels")
    conn.execute(
        """
        merge into raw.channels target
        using incoming_channels source
        on target.channel_id = source.channel_id
        when matched then update set
            channel_name = source.channel_name,
            creator_id = source.creator_id,
            is_archived = source.is_archived,
            is_general = source.is_general,
            created_ts = source.created_ts,
            members = source.members,
            purpose = source.purpose,
            topic = source.topic,
            pins = source.pins,
            source_id = source.source_id,
            last_seen_at = current_timestamp
        when not matched then insert (
            channel_id, channel_name, creator_id, is_archived, is_general, created_ts,
            members, purpose, topic, pins, source_id
        ) values (
            source.channel_id, source.channel_name, source.creator_id, source.is_archived,
            source.is_general, source.created_ts, source.members, source.purpose,
            source.topic, source.pins, source.source_id
        )
        """
    )
    emit(reporter, "[merge] messages")
    conn.execute(
        """
        merge into raw.messages target
        using incoming_messages source
        on target.message_key = source.message_key
        when matched then update set
            channel_id = source.channel_id,
            channel_name = source.channel_name,
            message_ts = source.message_ts,
            thread_ts = source.thread_ts,
            user_id = source.user_id,
            user_name = source.user_name,
            user_real_name = source.user_real_name,
            client_msg_id = source.client_msg_id,
            message_type = source.message_type,
            subtype = source.subtype,
            text = source.text,
            reply_count = source.reply_count,
            reply_users_count = source.reply_users_count,
            latest_reply_ts = source.latest_reply_ts,
            source_team = source.source_team,
            team = source.team,
            user_team = source.user_team,
            raw_payload = source.raw_payload,
            source_id = source.source_id,
            last_seen_at = current_timestamp
        when not matched then insert (
            message_key, channel_id, channel_name, message_ts, thread_ts, user_id, user_name,
            user_real_name, client_msg_id, message_type, subtype, text, reply_count,
            reply_users_count, latest_reply_ts, source_team, team, user_team, raw_payload, source_id
        ) values (
            source.message_key, source.channel_id, source.channel_name, source.message_ts,
            source.thread_ts, source.user_id, source.user_name, source.user_real_name,
            source.client_msg_id, source.message_type, source.subtype, source.text,
            source.reply_count, source.reply_users_count, source.latest_reply_ts,
            source.source_team, source.team, source.user_team, source.raw_payload, source.source_id
        )
        """
    )
    emit(reporter, "[merge] message_reactions")
    conn.execute(
        """
        merge into raw.message_reactions target
        using incoming_message_reactions source
        on target.reaction_key = source.reaction_key
        when matched then update set
            message_key = source.message_key,
            channel_id = source.channel_id,
            message_ts = source.message_ts,
            reaction_name = source.reaction_name,
            reaction_count = source.reaction_count,
            users = source.users,
            source_id = source.source_id,
            last_seen_at = current_timestamp
        when not matched then insert (
            reaction_key, message_key, channel_id, message_ts, reaction_name, reaction_count, users, source_id
        ) values (
            source.reaction_key, source.message_key, source.channel_id, source.message_ts,
            source.reaction_name, source.reaction_count, source.users, source.source_id
        )
        """
    )

    conn.unregister("incoming_ingest_sources")
    conn.unregister("incoming_users")
    conn.unregister("incoming_channels")
    conn.unregister("incoming_messages")
    conn.unregister("incoming_message_reactions")
    emit(
        reporter,
        f"[done] {metadata.source_name} total_elapsed={format_elapsed(monotonic() - started_at)}",
    )


def normalize_user(user: dict[str, Any], source_id: str) -> dict[str, Any]:
    return {
        "user_id": user.get("id"),
        "name": user.get("name"),
        "real_name": user.get("real_name") or user.get("profile", {}).get("real_name"),
        "is_bot": user.get("is_bot"),
        "is_deleted": user.get("deleted"),
        "tz": user.get("tz"),
        "updated_at": user.get("updated"),
        "profile": json.dumps(user.get("profile"), sort_keys=True),
        "source_id": source_id,
    }


def normalize_channel(channel: dict[str, Any], source_id: str) -> dict[str, Any]:
    return {
        "channel_id": channel.get("id"),
        "channel_name": channel.get("name"),
        "creator_id": channel.get("creator"),
        "is_archived": channel.get("is_archived"),
        "is_general": channel.get("is_general"),
        "created_ts": channel.get("created"),
        "members": json.dumps(channel.get("members", [])),
        "purpose": json.dumps(channel.get("purpose")),
        "topic": json.dumps(channel.get("topic")),
        "pins": json.dumps(channel.get("pins", [])),
        "source_id": source_id,
    }


def parse_messages(
    zf: ZipFile,
    source_id: str,
    reporter: Reporter | None = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], ParseStats]:
    messages: list[dict[str, Any]] = []
    reactions: list[dict[str, Any]] = []
    channel_lookup = load_channel_lookup(zf)
    started_at = monotonic()
    message_files = [
        info
        for info in zf.infolist()
        if not info.is_dir()
        and info.filename not in {"users.json", "channels.json"}
        and info.filename.count("/") == 1
        and info.filename.endswith(".json")
    ]
    total_files = len(message_files)
    emit(reporter, f"[parse] scanning {total_files} channel-day json files")

    for index, info in enumerate(message_files, start=1):
        parts = info.filename.split("/")
        channel_name = parts[0]
        payload = json.loads(zf.read(info.filename))
        if not isinstance(payload, list):
            continue
        channel_id = channel_lookup.get(channel_name)
        for record in payload:
            if not isinstance(record, dict):
                continue
            message = normalize_message(channel_name, channel_id, record, source_id)
            messages.append(message)
            reactions.extend(normalize_reactions(message, record.get("reactions") or [], source_id))
        if index == total_files or index == 1 or index % 250 == 0:
            percent = (index / total_files) * 100 if total_files else 100.0
            emit(
                reporter,
                f"[parse] {index}/{total_files} files ({percent:0.1f}%) "
                f"messages={len(messages)} reactions={len(reactions)} "
                f"elapsed={format_elapsed(monotonic() - started_at)} last={info.filename}",
            )
    return messages, reactions, ParseStats(
        channel_files=total_files,
        messages=len(messages),
        reactions=len(reactions),
    )


def load_channel_lookup(zf: ZipFile) -> dict[str, str]:
    channels = json.loads(zf.read("channels.json"))
    return {channel.get("name"): channel.get("id") for channel in channels if channel.get("name")}


def normalize_message(
    channel_name: str,
    channel_id: str | None,
    record: dict[str, Any],
    source_id: str,
) -> dict[str, Any]:
    message_ts = record.get("ts")
    user_profile = record.get("user_profile") or {}
    client_msg_id = record.get("client_msg_id")
    message_key = build_message_key(channel_id, channel_name, message_ts, client_msg_id, record)
    return {
        "message_key": message_key,
        "channel_id": channel_id,
        "channel_name": channel_name,
        "message_ts": str(message_ts) if message_ts is not None else None,
        "thread_ts": record.get("thread_ts"),
        "user_id": record.get("user"),
        "user_name": user_profile.get("name") or record.get("username"),
        "user_real_name": user_profile.get("real_name"),
        "client_msg_id": client_msg_id,
        "message_type": record.get("type"),
        "subtype": record.get("subtype"),
        "text": record.get("text"),
        "reply_count": record.get("reply_count"),
        "reply_users_count": record.get("reply_users_count"),
        "latest_reply_ts": record.get("latest_reply"),
        "source_team": record.get("source_team"),
        "team": record.get("team"),
        "user_team": record.get("user_team"),
        "raw_payload": json.dumps(record, sort_keys=True),
        "source_id": source_id,
    }


def normalize_reactions(
    message: dict[str, Any],
    reaction_records: list[dict[str, Any]],
    source_id: str,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for reaction in reaction_records:
        name = reaction.get("name")
        users = reaction.get("users", [])
        reaction_key = f"{message['message_key']}::{name}"
        rows.append(
            {
                "reaction_key": reaction_key,
                "message_key": message["message_key"],
                "channel_id": message["channel_id"],
                "message_ts": message["message_ts"],
                "reaction_name": name,
                "reaction_count": reaction.get("count"),
                "users": json.dumps(users),
                "source_id": source_id,
            }
        )
    return rows


def build_message_key(
    channel_id: str | None,
    channel_name: str,
    message_ts: str | None,
    client_msg_id: str | None,
    record: dict[str, Any],
) -> str:
    channel_token = channel_id or channel_name
    if message_ts:
        return f"{channel_token}:{message_ts}"
    if client_msg_id:
        return f"{channel_token}:client:{client_msg_id}"
    payload_hash = hashlib.sha256(json.dumps(record, sort_keys=True).encode("utf-8")).hexdigest()
    return f"{channel_token}:payload:{payload_hash}"


def emit(reporter: Reporter | None, message: str) -> None:
    if reporter is not None:
        reporter(message)


def format_bytes(size_bytes: int) -> str:
    size = float(size_bytes)
    for unit in ["B", "KiB", "MiB", "GiB", "TiB"]:
        if size < 1024.0 or unit == "TiB":
            return f"{size:0.1f}{unit}"
        size /= 1024.0
    return f"{size_bytes}B"


def format_elapsed(seconds: float) -> str:
    total = int(seconds)
    hours, remainder = divmod(total, 3600)
    minutes, secs = divmod(remainder, 60)
    if hours:
        return f"{hours:d}h{minutes:02d}m{secs:02d}s"
    if minutes:
        return f"{minutes:d}m{secs:02d}s"
    return f"{secs:d}s"
