from __future__ import annotations

import duckdb


def run_doctor(conn: duckdb.DuckDBPyConnection) -> dict[str, int]:
    return {
        "sources": conn.execute("select count(*) from raw.ingest_sources").fetchone()[0],
        "users": conn.execute("select count(*) from raw.users").fetchone()[0],
        "channels": conn.execute("select count(*) from raw.channels").fetchone()[0],
        "messages": conn.execute("select count(*) from raw.messages").fetchone()[0],
        "reactions": conn.execute("select count(*) from raw.message_reactions").fetchone()[0],
        "emoji_catalog_entries": conn.execute("select count(*) from raw.emoji_catalog").fetchone()[0],
        "emoji_lookup_rows": conn.execute("select count(*) from analytics.emoji_lookup").fetchone()[0],
        "messages_without_user": conn.execute(
            "select count(*) from raw.messages where user_id is null"
        ).fetchone()[0],
        "messages_without_text": conn.execute(
            "select count(*) from raw.messages where text is null or trim(text) = ''"
        ).fetchone()[0],
        "messages_without_channel_dim": conn.execute(
            """
            select count(*) from raw.messages m
            left join raw.channels c on m.channel_id = c.channel_id
            where m.channel_id is not null and c.channel_id is null
            """
        ).fetchone()[0],
        "messages_without_user_dim": conn.execute(
            """
            select count(*) from raw.messages m
            left join raw.users u on m.user_id = u.user_id
            where m.user_id is not null and u.user_id is null
            """
        ).fetchone()[0],
    }
