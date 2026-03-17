# Slack Analytics Rebuild Plan

## Summary
Rebuild this repo as a local-first Python project managed entirely by `uv`, with Slack export zip files in `exports/` as the canonical raw input, DuckDB as the canonical analytical store, and a marimo app as the primary inspection surface.

The system will ingest one or more full Slack workspace snapshot zips, normalize them into stable entity/event tables, and build aggregate marts for the analytics you care about: posts by year/month/user/channel, top posters, top channels, channel membership/size, reactions, and thread activity. Ingest will be snapshot-aware and idempotent: adding a newer export should upsert the canonical database rather than requiring a full drop/recreate workflow.

## Implementation Changes
### Project structure and tooling
- Replace the notebook-first workflow with a `uv` project that exposes all actions through `uv run ...`.
- Add a small package layout with three concerns:
  - ingest/parsing of Slack export zips
  - DuckDB schema build and incremental load
  - analytics marts and marimo app queries
- Keep the existing notebook as legacy/reference only; do not make it the source of truth for the new pipeline.
- Replace the oversized `requirements.txt` with `uv`-managed dependencies and a lockfile.
- Standardize commands such as:
  - `uv run slackalytics ingest`
  - `uv run slackalytics build-marts`
  - `uv run marimo run app.py`
  - optionally `uv run slackalytics doctor` for sanity checks

### Raw input and staging
- Treat files in `exports/` as immutable source snapshots.
- Do not track extracted raw folders as canonical data.
- On ingest, read zip contents directly when practical; allow a managed temporary/cache extraction path only if needed for parser simplicity or performance.
- Track each zip as an ingestion source with metadata such as filename, snapshot coverage dates, file hash, ingest timestamp, and parse status.
- Use the newer snapshot plus any older snapshots together as input history, but build one canonical dataset by deduplicating/upserting on stable Slack record identity.

### Canonical DuckDB model
- Keep DuckDB as the primary database.
- Create normalized base tables for at least:
  - `ingest_sources`
  - `channels`
  - `users`
  - `messages`
  - `message_reactions`
  - `message_replies` or equivalent thread linkage table
- Keep full message text in the canonical `messages` table.
- Preserve enough raw Slack identifiers to support stable upserts and future extensions:
  - channel id/name
  - user id/name/real name/bot flags
  - message ts
  - client message id when available
  - thread/root ts
  - snapshot source id
- Avoid stringified Python dict/list blobs in canonical modeled tables where the data is analytically meaningful. Flatten reactions and thread linkage into relational tables; keep less important raw payload fragments in JSON columns if needed.
- Remove CSV-export artifacts such as `Unnamed: 0` from the new schema.

### Incremental ingest / upsert behavior
- Use snapshot-merge semantics:
  - each zip is a full snapshot of workspace history as of export time
  - newer snapshots may contain all older messages plus additional history and metadata changes
- Upsert strategy:
  - users keyed by Slack user id
  - channels keyed by Slack channel id
  - messages keyed primarily by `(channel_id, ts)`; use fallback logic only if a record lacks channel id or ts in a way Slack exports make unavoidable
  - reactions keyed by message key plus reaction name and reacting user
- Allow metadata to refresh on newer snapshots:
  - user names/profile fields can update
  - channel membership/topic/purpose/archive state can update
- Treat missing records in a newer snapshot as non-destructive by default; do not delete historical canonical records unless there is a clearly modeled deletion rule.
- Make repeated ingest of the same zip a no-op aside from validation and metadata refresh checks.

### Analytics marts and marimo surface
- Build aggregate tables or views on top of the canonical model for:
  - posts by year
  - posts by month
  - posts by user
  - posts by user by month
  - posts by channel
  - posts by channel by month/year
  - top active channels
  - largest channels by member count
  - reactions by user/channel/message
  - thread counts, reply counts, and most-threaded channels/posts
- Prefer DuckDB views or materialized tables where query speed or simplicity benefits the marimo app.
- Build one main marimo app that provides:
  - overview metrics
  - sortable/filterable tables
  - time-series charts
  - channel and user drilldowns
  - simple query parameter controls for date range, channel, user, and inclusion/exclusion of bots
- Treat DuckDB as the export surface in v1; no dedicated CSV/HTML export layer beyond what DuckDB or marimo can already do.

## Public Interfaces / Commands
- CLI entrypoints should be stable and documented:
  - `ingest [--source <zip-or-dir>] [--db <path>]`
  - `build-marts [--db <path>]`
  - `app [--db <path>]` or direct marimo command using the same config
  - `doctor [--db <path>]` for schema/data sanity checks
- Configuration should be file-based and minimal:
  - default DB path
  - exports directory
  - optional cache/temp extraction directory
  - bot inclusion defaults for analytics
- The marimo app should read from the canonical DuckDB database only, not from raw zips or ad hoc CSVs.

## Test Plan
- Parser tests for representative Slack message shapes:
  - plain messages
  - messages with reactions
  - thread roots and replies
  - bot/system-ish messages
  - messages with missing text or missing user profile fragments
- Ingest idempotency tests:
  - ingest one zip twice produces no duplicate canonical records
  - ingest older then newer snapshot updates counts without duplicates
- Upsert tests:
  - changed user/channel metadata refreshes correctly
  - messages remain unique by canonical key
- Mart validation tests:
  - yearly/monthly/channel/user counts match known source-derived totals on a fixture export
  - reaction and thread aggregates reconcile to base tables
- Smoke tests:
  - `uv run ... ingest` on the sample exports completes successfully
  - marimo app opens and queries the built DB without requiring notebook state
- Data quality checks in `doctor`:
  - orphan messages without known channel/user are counted and reported
  - channels/users present in messages but absent from dimension tables are surfaced
  - ingest source history is traceable

## Assumptions and Defaults
- Default runtime is local-only on one machine.
- `uv` is the only supported workflow for environment management and command execution.
- DuckDB remains the database unless a concrete blocker appears during implementation.
- V1 includes the core summary analytics plus reactions and thread analytics.
- Raw export zips remain canonical; extracted folders are temporary implementation detail only.
- Full message text is retained in the modeled database because the repo is private/personal and future analytics will likely want it.
- No hosted deployment, auth layer, or multi-user sharing is included in v1.
