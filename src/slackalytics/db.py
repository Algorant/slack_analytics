from __future__ import annotations

import duckdb


SCHEMA_SQL = """
create schema if not exists raw;
create schema if not exists analytics;

create table if not exists raw.ingest_sources (
    source_id varchar primary key,
    source_name varchar not null,
    source_path varchar not null,
    file_hash varchar not null,
    source_size_bytes bigint not null,
    source_modified_at timestamp,
    exported_through date,
    ingested_at timestamp not null default current_timestamp
);

create table if not exists raw.users (
    user_id varchar primary key,
    name varchar,
    real_name varchar,
    is_bot boolean,
    is_deleted boolean,
    tz varchar,
    updated_at bigint,
    profile json,
    source_id varchar not null,
    last_seen_at timestamp not null default current_timestamp
);

create table if not exists raw.channels (
    channel_id varchar primary key,
    channel_name varchar,
    creator_id varchar,
    is_archived boolean,
    is_general boolean,
    created_ts bigint,
    members json,
    purpose json,
    topic json,
    pins json,
    source_id varchar not null,
    last_seen_at timestamp not null default current_timestamp
);

create table if not exists raw.messages (
    message_key varchar primary key,
    channel_id varchar,
    channel_name varchar not null,
    message_ts varchar,
    thread_ts varchar,
    user_id varchar,
    user_name varchar,
    user_real_name varchar,
    client_msg_id varchar,
    message_type varchar,
    subtype varchar,
    text varchar,
    reply_count integer,
    reply_users_count integer,
    latest_reply_ts varchar,
    source_team varchar,
    team varchar,
    user_team varchar,
    raw_payload json,
    source_id varchar not null,
    last_seen_at timestamp not null default current_timestamp
);

create table if not exists raw.message_reactions (
    reaction_key varchar primary key,
    message_key varchar not null,
    channel_id varchar,
    message_ts varchar,
    reaction_name varchar not null,
    reaction_count integer,
    users json,
    source_id varchar not null,
    last_seen_at timestamp not null default current_timestamp
);

create table if not exists raw.emoji_catalog (
    emoji_name varchar primary key,
    raw_value varchar not null,
    synced_at timestamp not null default current_timestamp
);

create table if not exists analytics.emoji_lookup (
    emoji_name varchar primary key,
    emoji_kind varchar not null,
    display_value varchar not null,
    display_name varchar not null,
    unicode_glyph varchar,
    image_url varchar,
    alias_target varchar,
    resolved_name varchar,
    is_custom boolean not null default false,
    synced_at timestamp not null default current_timestamp
);

create or replace view analytics.message_facts as
select
    m.message_key,
    m.channel_id,
    m.channel_name,
    m.message_ts,
    try_cast(m.message_ts as double) as message_ts_seconds,
    to_timestamp(try_cast(m.message_ts as double)) as message_time,
    date_trunc('month', to_timestamp(try_cast(m.message_ts as double))) as message_month,
    date_trunc('year', to_timestamp(try_cast(m.message_ts as double))) as message_year,
    m.thread_ts,
    m.user_id,
    coalesce(m.user_real_name, m.user_name, m.user_id, 'Unknown') as user_label,
    m.message_type,
    m.subtype,
    coalesce(m.reply_count, 0) as reply_count,
    coalesce(length(trim(m.text)), 0) as text_length
from raw.messages m;

create or replace view analytics.posts_by_year as
select
    cast(extract(year from message_time) as integer) as year,
    count(*) as post_count
from analytics.message_facts
where message_time is not null
group by 1
order by 1;

create or replace view analytics.posts_by_month as
select
    cast(message_month as date) as month,
    count(*) as post_count
from analytics.message_facts
where message_month is not null
group by 1
order by 1;

create or replace view analytics.posts_by_user as
select
    user_label,
    user_id,
    count(*) as post_count
from analytics.message_facts
group by 1, 2
order by post_count desc, user_label;

create or replace view analytics.posts_by_user_month as
select
    cast(message_month as date) as month,
    user_label,
    user_id,
    count(*) as post_count
from analytics.message_facts
where message_month is not null
group by 1, 2, 3
order by month, post_count desc, user_label;

create or replace view analytics.posts_by_channel as
select
    channel_name,
    channel_id,
    count(*) as post_count
from analytics.message_facts
group by 1, 2
order by post_count desc, channel_name;

create or replace view analytics.posts_by_channel_month as
select
    cast(message_month as date) as month,
    channel_name,
    channel_id,
    count(*) as post_count
from analytics.message_facts
where message_month is not null
group by 1, 2, 3
order by month, post_count desc, channel_name;

create or replace view analytics.channel_sizes as
select
    channel_name,
    channel_id,
    json_array_length(members) as member_count,
    is_archived,
    to_timestamp(created_ts) as created_at
from raw.channels
order by member_count desc nulls last, channel_name;

create or replace view analytics.reactions_by_channel as
select
    m.channel_name,
    m.channel_id,
    count(*) as reacted_messages,
    sum(coalesce(r.reaction_count, 0)) as total_reactions
from raw.messages m
join raw.message_reactions r on m.message_key = r.message_key
group by 1, 2
order by total_reactions desc, channel_name;

create or replace view analytics.thread_activity_by_channel as
select
    channel_name,
    channel_id,
    count(*) filter (where coalesce(reply_count, 0) > 0) as threaded_posts,
    sum(coalesce(reply_count, 0)) as total_replies
from analytics.message_facts
group by 1, 2
order by total_replies desc, threaded_posts desc, channel_name;
"""


def connect(database_path: str) -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect(database_path)
    conn.execute("set preserve_insertion_order = false")
    return conn


def ensure_schema(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute(SCHEMA_SQL)
