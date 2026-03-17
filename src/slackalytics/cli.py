from __future__ import annotations

from pathlib import Path
from time import monotonic

import typer

from slackalytics.config import Settings
from slackalytics.db import connect, ensure_schema
from slackalytics.doctor import run_doctor
from slackalytics.emojis import SlackEmojiSyncError, sync_emojis
from slackalytics.ingest import ingest_exports


app = typer.Typer(no_args_is_help=True, add_completion=False)


@app.command()
def ingest(
    db: Path | None = typer.Option(None, help="DuckDB database path."),
    exports_dir: Path | None = typer.Option(None, help="Directory containing Slack export zip files."),
) -> None:
    settings = Settings.discover()
    db_path = db or settings.database_path
    source_dir = exports_dir or settings.exports_dir
    started_at = monotonic()
    conn = connect(str(db_path))
    try:
        sources = ingest_exports(conn, source_dir, reporter=typer.echo)
    finally:
        conn.close()
    typer.echo(
        f"Ingested {len(sources)} source snapshot(s) into {db_path} "
        f"in {monotonic() - started_at:0.1f}s"
    )


@app.command("build-marts")
def build_marts(db: Path | None = typer.Option(None, help="DuckDB database path.")) -> None:
    settings = Settings.discover()
    db_path = db or settings.database_path
    conn = connect(str(db_path))
    try:
        ensure_schema(conn)
    finally:
        conn.close()
    typer.echo(f"Analytics views refreshed in {db_path}")


@app.command()
def doctor(db: Path | None = typer.Option(None, help="DuckDB database path.")) -> None:
    settings = Settings.discover()
    db_path = db or settings.database_path
    conn = connect(str(db_path))
    try:
        ensure_schema(conn)
        results = run_doctor(conn)
    finally:
        conn.close()
    for key, value in results.items():
        typer.echo(f"{key}: {value}")


@app.command("sync-emojis")
def sync_emojis_command(
    db: Path | None = typer.Option(None, help="DuckDB database path."),
) -> None:
    settings = Settings.discover()
    db_path = db or settings.database_path
    conn = connect(str(db_path))
    try:
        ensure_schema(conn)
        catalog_count, lookup_count = sync_emojis(conn, token=settings.slack_token)
    except SlackEmojiSyncError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(code=1)
    finally:
        conn.close()
    typer.echo(
        f"Synced {catalog_count} Slack emoji catalog entries and {lookup_count} lookup rows into {db_path}"
    )
