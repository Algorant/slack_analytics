# AGENTS.md

## Project purpose
- This project analyzes a private personal Slack workspace export used as a group chat among friends.
- The goal is practical analytics over Slack history: posts by year/month/user/channel, active channels, reaction/thread activity, and similar exploratory summaries.
- Treat this as a local-first analytics project, not a general-purpose web service.

## Privacy and data handling
- The data is personal and private.
- Do not suggest publishing raw exports, message text, profiles, or derived databases unless explicitly asked.
- Prefer local tooling, local files, and local database workflows by default.

## Environment and workflow
- Use `uv` for Python-related work: dependency resolution, virtualenv management, running Python, tests, scripts, and app commands.
- The canonical raw inputs are Slack export zip files in `exports/`.
- The canonical database is `slackalytics.duckdb`.
- The current UI surface is marimo via `uv run marimo run app.py`.
- The current CLI entrypoint is `uv run slackalytics ...`.
- Local secrets/config can live in a repo-root `.env`; `SLACK_TOKEN` should be read from there for emoji sync.

## Data model expectations
- Slack exports are full snapshot zips, not append-only event logs.
- Ingest should remain snapshot-aware and idempotent.
- DuckDB is the default database unless there is a concrete blocker.
- Keep full message text in the canonical message table unless explicitly asked to remove or anonymize it.

## Primary users
- There are many users in the export tables, including bots and Slack system users.
- For most human-facing analytics, the primary focus should be these 8 users:
  - Ivan
  - Al / Alejandro
  - Stephen
  - Will
  - Ben
  - Michael
  - Derek
  - Roth
- When producing summaries, charts, filters, or defaults for “top users” analysis, prefer centering these 8 primary users.
- Do not silently delete or discard other users from canonical storage; instead, treat them as secondary users, bots, or system actors unless the task is specifically about the primary 8.

## Interpretation defaults
- Bots and system identities such as `USLACKBOT` may exist in the export and should usually be excluded from “core friend group” analytics unless explicitly requested.
- Functional Slack channel artifacts may appear with `FC:...` names; treat them as export quirks rather than core human-created channels.
- If there is a tradeoff between preserving raw fidelity and making friend-group analytics usable, preserve the raw data in canonical tables and apply friend-group filtering in marts, queries, or UI defaults.

## Implementation preferences
- Avoid notebook-only logic for canonical workflows.
- Prefer clear CLI commands, stable DuckDB views/tables, and a marimo surface for inspection.
- Add progress output for long-running ingest operations.
- If something important is not working, stop and state the exact problem rather than inventing a brittle workaround.
- Before saying app changes are done, verify the actual marimo app runs without runtime errors, not just that static checks pass.
- For `app.py` changes, run `uv run marimo check app.py`, `uv run pytest`, and the Playwright smoke test in `tests/test_marimo_smoke.py`. Do not treat a plain headless marimo startup as sufficient validation for browser-visible UI changes.
