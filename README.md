# Slackalytics

Local-first analytics for Slack workspace export snapshots.

## Workflow
- Put raw Slack export zip files in `exports/`.
- Ingest them into DuckDB with `uv run slackalytics ingest`.
- Refresh analytics views with `uv run slackalytics build-marts`.
- Put `SLACK_TOKEN=...` in a repo-root `.env` file if you want Slack API emoji enrichment.
- Sync emoji metadata with `uv run slackalytics sync-emojis`.
- Inspect the results in marimo with `uv run marimo run app.py`.

## Commands
```bash
uv run slackalytics ingest
uv run slackalytics build-marts
uv run slackalytics sync-emojis
uv run slackalytics doctor
uv run marimo run app.py
```
