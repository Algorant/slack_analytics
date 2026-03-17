import marimo

__generated_with = "0.20.4"
app = marimo.App(width="full")


@app.cell
def _():
    from datetime import date
    from math import isnan

    import altair as alt
    import duckdb
    import marimo as mo

    from slackalytics.dashboard import canonical_label_sql, word_count_sql
    from slackalytics.people import (
        PRIMARY_USER_IDS,
        PRIMARY_USER_LABELS,
        PRIMARY_USERS,
        SYSTEM_USER_IDS,
    )

    def filters_sql(
        *,
        primary_only: bool,
        include_bots: bool,
        include_functional_channels: bool,
        start_date: date | None,
        end_date: date | None,
        selected_channels: list[str],
    ) -> tuple[str, list[object]]:
        clauses: list[str] = []
        params: list[object] = []

        if primary_only:
            placeholders = ", ".join(["?"] * len(PRIMARY_USER_IDS))
            clauses.append(f"m.user_id in ({placeholders})")
            params.extend(PRIMARY_USER_IDS)

        if not include_bots:
            placeholders = ", ".join(["?"] * len(SYSTEM_USER_IDS))
            clauses.append("coalesce(u.is_bot, false) = false")
            clauses.append(f"coalesce(m.user_id, '') not in ({placeholders})")
            params.extend(SYSTEM_USER_IDS)

        if not include_functional_channels:
            clauses.append("not starts_with(m.channel_name, 'FC:')")

        if start_date is not None:
            clauses.append("cast(m.message_time as date) >= ?")
            params.append(start_date.isoformat())

        if end_date is not None:
            clauses.append("cast(m.message_time as date) <= ?")
            params.append(end_date.isoformat())

        if selected_channels:
            placeholders = ", ".join(["?"] * len(selected_channels))
            clauses.append(f"m.channel_name in ({placeholders})")
            params.extend(selected_channels)

        where_clause = " and ".join(clauses) if clauses else "true"
        return where_clause, params

    def query_df(
        conn: duckdb.DuckDBPyConnection,
        sql: str,
        params: list[object] | None = None,
    ):
        return conn.execute(sql, params or []).fetchdf()

    def normalize_image_url(value) -> str | None:
        if value is None:
            return None
        if isinstance(value, float) and isnan(value):
            return None
        if isinstance(value, str):
            stripped = value.strip()
            return stripped or None
        return None

    def emoji_preview_cell(
        mo,
        *,
        emoji_name: str,
        unicode_glyph: str | None,
        image_url: str | None,
        size: int = 18,
    ):
        image_url = normalize_image_url(image_url)
        if image_url is not None:
            return mo.image(image_url, alt=emoji_name, width=size, height=size)
        if unicode_glyph:
            return unicode_glyph
        return f":{emoji_name}:"

    def render_table(
        mo,
        data,
        *,
        label: str,
        page_size: int = 15,
        pagination: bool = True,
        wrapped_columns: list[str] | None = None,
    ):
        return mo.ui.table(
            data,
            label=label,
            pagination=pagination,
            page_size=page_size,
            show_column_summaries=True,
            show_data_types=False,
            wrapped_columns=wrapped_columns,
        )

    def pivot_people_matrix(
        data,
        *,
        index_column: str,
        column_column: str,
        value_column: str,
        sort_columns: list[str] | None = None,
        drop_columns: list[str] | None = None,
    ):
        working = data.copy()
        if sort_columns:
            working = working.sort_values(sort_columns)
        pivoted = (
            working.pivot(index=index_column, columns=column_column, values=value_column)
            .fillna(0)
            .astype(int)
            .reset_index()
        )
        pivoted.columns.name = None
        if drop_columns:
            pivoted = pivoted.drop(columns=drop_columns, errors="ignore")
        return pivoted

    def pivot_rank_matrix(
        data,
        *,
        index_column: str,
        rank_column: str,
        value_column: str,
        max_rank: int,
    ):
        pivoted = data.pivot(index=index_column, columns=rank_column, values=value_column).reset_index()
        pivoted.columns.name = None
        for rank in range(1, max_rank + 1):
            column = f"Top {rank}"
            if column not in pivoted.columns:
                pivoted[column] = ""
        ordered_columns = [index_column] + [f"Top {rank}" for rank in range(1, max_rank + 1)]
        return pivoted[ordered_columns].fillna("")

    def sort_people_rows(data, person_column: str = "Person"):
        if person_column not in data.columns:
            return data
        order = {user["label"]: index for index, user in enumerate(PRIMARY_USERS)}
        return (
            data.assign(
                _person_sort=data[person_column].map(lambda value: order.get(value, len(order)))
            )
            .sort_values(["_person_sort", person_column])
            .drop(columns=["_person_sort"])
            .reset_index(drop=True)
        )

    return (
        PRIMARY_USER_IDS,
        PRIMARY_USER_LABELS,
        PRIMARY_USERS,
        SYSTEM_USER_IDS,
        alt,
        canonical_label_sql,
        date,
        duckdb,
        filters_sql,
        mo,
        emoji_preview_cell,
        pivot_people_matrix,
        pivot_rank_matrix,
        query_df,
        render_table,
        sort_people_rows,
        word_count_sql,
    )


@app.cell
def _(duckdb):
    db_path = "slackalytics.duckdb"
    conn = duckdb.connect(db_path, read_only=True)
    return conn, db_path


@app.cell
def _(canonical_label_sql, conn, db_path, mo, query_df):
    summary = query_df(
        conn,
        f"""
        select
            count(*) as total_messages,
            count(distinct channel_name) as total_channels,
            count(distinct {canonical_label_sql('m', 'u')}) as total_user_identities,
            cast(min(message_time) as date) as first_message_date,
            cast(max(message_time) as date) as last_message_date
        from analytics.message_facts m
        left join raw.users u on m.user_id = u.user_id
        where message_time is not null
        """,
    )
    mo.vstack(
        [
            mo.md(
                f"""
                # Slackalytics

                Basic local analytics app for the private Slack workspace.

                Database: `{db_path}`
                """
            ),
            mo.hstack(
                [
                    mo.stat(int(summary.iloc[0]["total_messages"]), label="Messages", bordered=True),
                    mo.stat(int(summary.iloc[0]["total_channels"]), label="Channels", bordered=True),
                    mo.stat(
                        int(summary.iloc[0]["total_user_identities"]),
                        label="People recognized",
                        bordered=True,
                    ),
                    mo.stat(
                        str(summary.iloc[0]["first_message_date"]),
                        label="First message",
                        bordered=True,
                    ),
                    mo.stat(
                        str(summary.iloc[0]["last_message_date"]),
                        label="Latest message",
                        bordered=True,
                    ),
                ],
                widths="equal",
                gap=1,
            ),
        ],
        gap=1,
    )
    return (summary,)


@app.cell
def _(canonical_label_sql, conn, query_df):
    bounds = query_df(
        conn,
        """
        select
            cast(min(message_time) as date) as start_date,
            cast(max(message_time) as date) as end_date
        from analytics.message_facts
        where message_time is not null
        """,
    )
    channels = query_df(
        conn,
        """
        select channel_name
        from analytics.posts_by_channel
        where channel_name is not null
        order by post_count desc, channel_name
        """,
    )["channel_name"].tolist()
    users = query_df(
        conn,
        f"""
        select person
        from (
            select
                {canonical_label_sql('m', 'u')} as person,
                count(*) as posts
            from analytics.message_facts m
            left join raw.users u on m.user_id = u.user_id
            group by 1
        )
        where person is not null
        order by posts desc, person
        """,
    )["person"].tolist()
    return bounds, channels, users


@app.cell
def _(bounds, channels, mo, users):
    # DuckDB -> pandas date results arrive as pandas.Timestamp; marimo's
    # date_range expects Python date objects or ISO date strings.
    default_start = bounds.iloc[0]["start_date"].date()
    default_end = bounds.iloc[0]["end_date"].date()
    focus_scope = mo.ui.dropdown(
        options={"Primary 8": "primary", "All users": "all"},
        value="Primary 8",
        label="User scope",
    )
    include_bots = mo.ui.switch(value=False, label="Include bots / system users")
    include_functional_channels = mo.ui.switch(value=False, label="Include FC:* channels")
    date_range = mo.ui.date_range(
        start=default_start,
        stop=default_end,
        value=(default_start, default_end),
        label="Date range",
    )
    selected_channels = mo.ui.multiselect(
        options=channels,
        value=[],
        label="Channels",
        full_width=True,
    )
    selected_users = mo.ui.multiselect(
        options=users,
        value=[],
        label="People",
        full_width=True,
    )

    mo.vstack(
        [
            mo.md("## Filters"),
            mo.hstack(
                [
                    focus_scope,
                    include_bots,
                    include_functional_channels,
                    date_range,
                ],
                wrap=True,
                gap=1,
            ),
            mo.hstack(
                [
                    selected_users,
                    selected_channels,
                ],
                widths="equal",
                gap=1,
                align="stretch",
            ),
        ],
        gap=0.75,
    )
    return (
        date_range,
        focus_scope,
        include_bots,
        include_functional_channels,
        selected_channels,
        selected_users,
    )


@app.cell
def _(
    PRIMARY_USER_LABELS,
    PRIMARY_USER_IDS,
    canonical_label_sql,
    conn,
    date_range,
    emoji_preview_cell,
    filters_sql,
    focus_scope,
    include_bots,
    include_functional_channels,
    mo,
    pivot_people_matrix,
    pivot_rank_matrix,
    query_df,
    selected_channels,
    selected_users,
    sort_people_rows,
    SYSTEM_USER_IDS,
    word_count_sql,
):
    start_date, end_date = date_range.value
    apply_primary_selection = focus_scope.value == "primary"
    primary_user_labels_sql = ", ".join(f"'{label}'" for label in PRIMARY_USER_LABELS)
    base_where_clause, params = filters_sql(
        primary_only=apply_primary_selection,
        include_bots=include_bots.value,
        include_functional_channels=include_functional_channels.value,
        start_date=start_date,
        end_date=end_date,
        selected_channels=list(selected_channels.value),
    )
    user_filter_clause = "true"
    if not apply_primary_selection and selected_users.value:
        placeholders = ", ".join(["?"] * len(selected_users.value))
        user_filter_clause = f"canonical_user_label in ({placeholders})"
        params.extend(list(selected_users.value))

    reaction_scope_params = params.copy()
    reaction_scope_clauses: list[str] = []
    if apply_primary_selection:
        placeholders = ", ".join(["?"] * len(PRIMARY_USER_IDS))
        reaction_scope_clauses.append(f"re.user_id in ({placeholders})")
        reaction_scope_params.extend(PRIMARY_USER_IDS)
    if not include_bots.value:
        placeholders = ", ".join(["?"] * len(SYSTEM_USER_IDS))
        reaction_scope_clauses.append("coalesce(ru.is_bot, false) = false")
        reaction_scope_clauses.append(f"coalesce(re.user_id, '') not in ({placeholders})")
        reaction_scope_params.extend(SYSTEM_USER_IDS)
    if not apply_primary_selection and selected_users.value:
        placeholders = ", ".join(["?"] * len(selected_users.value))
        reaction_scope_clauses.append(f"reacting_person in ({placeholders})")
        reaction_scope_params.extend(list(selected_users.value))
    reaction_scope_where = " and ".join(reaction_scope_clauses) if reaction_scope_clauses else "true"

    message_cte = f"""
    with base_messages as (
        select
            m.*,
            coalesce(u.is_bot, false) as is_bot,
            {canonical_label_sql('m', 'u')} as canonical_user_label,
            rm.text as raw_text,
            {word_count_sql('rm.text')} as word_count
        from analytics.message_facts m
        left join raw.users u on m.user_id = u.user_id
        left join raw.messages rm on m.message_key = rm.message_key
        where {base_where_clause}
    ),
    filtered_messages as (
        select *
        from base_messages
        where {user_filter_clause}
    )
    """
    reaction_cte = message_cte + f"""
    ,
    reaction_events as (
        select
            fm.message_key,
            fm.channel_name,
            trim(both '"' from je.value::varchar) as user_id,
            r.reaction_name
        from filtered_messages fm
        join raw.message_reactions r on fm.message_key = r.message_key
        cross join json_each(r.users) je
    ),
    filtered_reaction_events as (
        select
            re.*,
            {canonical_label_sql('re', 'ru', fallback_label_expr='null')} as reacting_person
        from reaction_events re
        left join raw.users ru on re.user_id = ru.user_id
        where {reaction_scope_where}
    )
    """

    kpis = query_df(
        conn,
        message_cte
        + """
        select
            count(*) as post_count,
            count(distinct channel_name) as channel_count,
            count(distinct canonical_user_label) as user_count,
            cast(min(message_time) as date) as first_message_date,
            cast(max(message_time) as date) as last_message_date,
            sum(reply_count) as total_replies
        from filtered_messages
        """,
        params,
    )
    posts_by_year = query_df(
        conn,
        message_cte
        + """
        select cast(cast(extract(year from message_time) as integer) as varchar) as "Year",
               count(*) as "Posts"
        from filtered_messages
        where message_time is not null
        group by 1
        order by 1
        """,
        params,
    )
    posts_by_month = query_df(
        conn,
        message_cte
        + """
        select strftime(date_trunc('month', message_time), '%b %Y') as "Month",
               count(*) as "Posts",
               cast(date_trunc('month', message_time) as date) as month_sort
        from filtered_messages
        where message_time is not null
        group by 1, 3
        order by month_sort desc
        limit 36
        """,
        params,
    ).drop(columns=["month_sort"])
    top_users = query_df(
        conn,
        message_cte
        + """
        select canonical_user_label as "Person",
               count(*) as "Posts",
               round(
                   count(*)::double
                   / greatest(date_diff('month', min(message_time), max(message_time)) + 1, 1),
                   1
               ) as "Posts / month",
               round(
                   count(*)::double
                   / greatest(date_diff('year', min(message_time), max(message_time)) + 1, 1),
                   1
               ) as "Posts / year",
               median(word_count) as "Median words",
               round(avg(word_count), 1) as "Avg words",
               sum(reply_count) as "Replies started"
        from filtered_messages
        group by 1
        order by "Posts" desc, "Person"
        limit 25
        """,
        params,
    )
    user_posts_by_year = query_df(
        conn,
        message_cte
        + f"""
        select
            canonical_user_label as "Person",
            cast(extract(year from message_time) as integer) as year_sort,
            cast(cast(extract(year from message_time) as integer) as varchar) as "Year",
            count(*) as "Posts"
        from filtered_messages
        where message_time is not null
          and canonical_user_label in ({primary_user_labels_sql})
        group by 1, 2, 3
        order by year_sort, "Person"
        """,
        params,
    )
    user_posts_by_month = query_df(
        conn,
        message_cte
        + f"""
        select
            canonical_user_label as "Person",
            cast(date_trunc('month', message_time) as date) as month_sort,
            strftime(date_trunc('month', message_time), '%b %Y') as "Month",
            count(*) as "Posts"
        from filtered_messages
        where message_time is not null
          and canonical_user_label in ({primary_user_labels_sql})
        group by 1, 2, 3
        order by month_sort, "Person"
        """,
        params,
    )
    user_posts_by_year_matrix = pivot_people_matrix(
        user_posts_by_year,
        index_column="Person",
        column_column="Year",
        value_column="Posts",
        sort_columns=["Person", "year_sort"],
        drop_columns=["year_sort"],
    )
    user_posts_by_year_matrix = sort_people_rows(user_posts_by_year_matrix)
    latest_months = (
        user_posts_by_month[["month_sort", "Month"]]
        .drop_duplicates()
        .sort_values("month_sort", ascending=False)
        .head(12)
        .sort_values("month_sort")
    )
    user_posts_by_month_recent = user_posts_by_month.merge(
        latest_months,
        on=["month_sort", "Month"],
        how="inner",
    )
    user_posts_by_month_matrix = pivot_people_matrix(
        user_posts_by_month_recent,
        index_column="Person",
        column_column="Month",
        value_column="Posts",
        sort_columns=["Person", "month_sort"],
        drop_columns=["month_sort"],
    )
    user_posts_by_month_matrix = sort_people_rows(user_posts_by_month_matrix)
    top_channels = query_df(
        conn,
        message_cte
        + """
        select channel_name as "Channel",
               count(*) as "Posts",
               sum(reply_count) as "Replies"
        from filtered_messages
        group by 1
        order by "Posts" desc, "Channel"
        limit 25
        """,
        params,
    )
    reactions = query_df(
        conn,
        reaction_cte
        + """
        select
            reacting_person as "Person",
            count(distinct message_key) as "Reacted messages",
            count(*) as "Total reactions"
        from filtered_reaction_events
        group by 1
        order by "Total reactions" desc, "Person"
        limit 25
        """,
        reaction_scope_params,
    )
    reactions = sort_people_rows(reactions)
    thread_activity = query_df(
        conn,
        message_cte
        + """
        select
            canonical_user_label as "Person",
            count(*) filter (where coalesce(reply_count, 0) > 0) as "Threaded posts",
            sum(reply_count) as "Total replies"
        from filtered_messages
        group by 1
        order by "Total replies" desc, "Threaded posts" desc, "Person"
        limit 25
        """,
        params,
    )
    thread_activity = sort_people_rows(thread_activity)
    top_emojis = query_df(
        conn,
        reaction_cte
        + """
        select
            reaction_name as "Name",
            coalesce(lookup.unicode_glyph, concat(':', reaction_name, ':')) as "Emoji",
            lookup.unicode_glyph as unicode_glyph,
            lookup.image_url as image_url,
            reaction_name as emoji_name_sort,
            count(*) as "Uses",
            count(distinct reacting_person) as "People",
            count(distinct message_key) as "Messages"
        from filtered_reaction_events
        left join analytics.emoji_lookup lookup on reaction_name = lookup.emoji_name
        group by 1, 2, 3, 4, 5
        order by "Uses" desc, emoji_name_sort
        limit 25
        """,
        reaction_scope_params,
    )
    top_emojis["Preview"] = top_emojis.apply(
        lambda preview_row: emoji_preview_cell(
            mo,
            emoji_name=preview_row["Name"],
            unicode_glyph=preview_row["unicode_glyph"],
            image_url=preview_row["image_url"],
        ),
        axis=1,
    )
    top_emojis = top_emojis[["Preview", "Name", "Emoji", "Uses", "People", "Messages", "emoji_name_sort"]]
    emoji_favorites_detail = query_df(
        conn,
        reaction_cte
        + """
        , emoji_counts as (
            select
                reacting_person as "Person",
                fre.reaction_name as "Name",
                lookup.unicode_glyph as unicode_glyph,
                lookup.image_url as image_url,
                fre.reaction_name as emoji_name_sort,
                count(*) as "Uses"
            from filtered_reaction_events fre
            left join analytics.emoji_lookup lookup on fre.reaction_name = lookup.emoji_name
            group by 1, 2, 3, 4, 5
        )
        select
            "Person",
            concat('Top ', row_number() over (
                partition by "Person"
                order by "Uses" desc, emoji_name_sort
            )) as "Rank",
            "Name",
            unicode_glyph,
            image_url,
            "Uses"
        from emoji_counts
        qualify row_number() over (
            partition by "Person"
            order by "Uses" desc, emoji_name_sort
        ) <= 5
        order by "Person", "Rank"
        """,
        reaction_scope_params,
    )
    emoji_favorites_detail["Preview"] = emoji_favorites_detail.apply(
        lambda preview_row: emoji_preview_cell(
            mo,
            emoji_name=preview_row["Name"],
            unicode_glyph=preview_row["unicode_glyph"],
            image_url=preview_row["image_url"],
        ),
        axis=1,
    )
    emoji_favorites = pivot_rank_matrix(
        emoji_favorites_detail[["Person", "Rank", "Preview"]],
        index_column="Person",
        rank_column="Rank",
        value_column="Preview",
        max_rank=5,
    )
    emoji_favorites = sort_people_rows(emoji_favorites)
    emoji_usage_by_person = query_df(
        conn,
        reaction_cte
        + f"""
        , top_emoji_set as (
            select reaction_name
            from filtered_reaction_events
            group by 1
            order by count(*) desc, reaction_name
            limit 12
        )
        select
            reacting_person as "Person",
            fre.reaction_name as "Name",
            coalesce(lookup.unicode_glyph, concat(':', fre.reaction_name, ':')) as "Emoji Label",
            lookup.unicode_glyph as unicode_glyph,
            lookup.image_url as image_url,
            fre.reaction_name as emoji_name_sort,
            count(*) as "Uses"
        from filtered_reaction_events fre
        join top_emoji_set tes on fre.reaction_name = tes.reaction_name
        left join analytics.emoji_lookup lookup on fre.reaction_name = lookup.emoji_name
        where reacting_person in ({primary_user_labels_sql})
        group by 1, 2, 3, 4, 5, 6
        order by "Person", "Uses" desc, emoji_name_sort
        """,
        reaction_scope_params,
    )
    emoji_usage_by_person["Emoji"] = emoji_usage_by_person.apply(
        lambda preview_row: emoji_preview_cell(
            mo,
            emoji_name=preview_row["Name"],
            unicode_glyph=preview_row["unicode_glyph"],
            image_url=preview_row["image_url"],
        ),
        axis=1,
    )
    emoji_usage_by_person_matrix = pivot_people_matrix(
        emoji_usage_by_person[["Person", "Emoji Label", "Uses"]],
        index_column="Person",
        column_column="Emoji Label",
        value_column="Uses",
        sort_columns=["Person", "Emoji Label"],
    )
    emoji_usage_by_person_matrix = sort_people_rows(emoji_usage_by_person_matrix)
    top_emojis = top_emojis.drop(columns=["emoji_name_sort"])
    return (
        apply_primary_selection,
        message_cte,
        reaction_cte,
        emoji_favorites,
        emoji_favorites_detail,
        emoji_usage_by_person,
        emoji_usage_by_person_matrix,
        kpis,
        params,
        posts_by_month,
        posts_by_year,
        reactions,
        reaction_scope_params,
        thread_activity,
        top_emojis,
        top_channels,
        top_users,
        user_posts_by_month_matrix,
        user_posts_by_month_recent,
        user_posts_by_month,
        user_posts_by_year_matrix,
        user_posts_by_year,
    )


@app.cell
def _(apply_primary_selection, kpis, mo):
    row = kpis.iloc[0]
    scope_label = "Primary 8" if apply_primary_selection else "Filtered all-user scope"
    mo.vstack(
        [
            mo.md(f"## Dashboard\n\nCurrent scope: **{scope_label}**"),
            mo.hstack(
                [
                    mo.stat(int(row["post_count"]), label="Posts in scope", bordered=True),
                    mo.stat(int(row["channel_count"]), label="Channels in scope", bordered=True),
                    mo.stat(int(row["user_count"]), label="Users in scope", bordered=True),
                    mo.stat(int(row["total_replies"] or 0), label="Replies", bordered=True),
                    mo.stat(str(row["first_message_date"]), label="Start", bordered=True),
                    mo.stat(str(row["last_message_date"]), label="End", bordered=True),
                ],
                widths="equal",
                wrap=True,
                gap=1,
            ),
        ],
        gap=1,
    )
    return


@app.cell
def _(mo, posts_by_month, posts_by_year, render_table):
    mo.accordion(
        {
            "Posts by year": render_table(
                mo,
                posts_by_year,
                label="Posts by year",
                pagination=False,
                page_size=20,
            ),
            "Posts by month (latest 36 months)": render_table(
                mo,
                posts_by_month,
                label="Posts by month",
                page_size=12,
            ),
        },
        multiple=True,
    )
    return


@app.cell
def _(alt, emoji_usage_by_person, mo, top_emojis, user_posts_by_month, user_posts_by_year):
    yearly_chart = (
        alt.Chart(user_posts_by_year)
        .mark_line(point=True)
        .encode(
            x=alt.X("year_sort:O", title="Year", sort=None),
            y=alt.Y("Posts:Q", title="Posts"),
            color=alt.Color("Person:N", title="Person"),
            tooltip=["Person:N", "Year:N", "Posts:Q"],
        )
        .properties(height=320, title="Posts per year for the primary 8")
    )
    monthly_chart = (
        alt.Chart(user_posts_by_month)
        .mark_line()
        .encode(
            x=alt.X("month_sort:T", title="Month"),
            y=alt.Y("Posts:Q", title="Posts"),
            color=alt.Color("Person:N", title="Person"),
            tooltip=["Person:N", "Month:N", "Posts:Q"],
        )
        .properties(height=320, title="Posts per month for the primary 8")
    )
    emoji_chart = (
        alt.Chart(top_emojis.head(15))
        .mark_bar()
        .encode(
            x=alt.X("Uses:Q", title="Reaction uses"),
            y=alt.Y("Emoji:N", sort="-x", title="Emoji"),
            tooltip=["Emoji:N", "Uses:Q", "People:Q", "Messages:Q"],
        )
        .properties(height=360, title="Top reaction emojis")
    )
    emoji_heatmap = (
        alt.Chart(emoji_usage_by_person[["Person", "Emoji Label", "Uses"]])
        .mark_rect()
        .encode(
            x=alt.X("Emoji Label:N", title="Emoji", sort="-y"),
            y=alt.Y("Person:N", title="Person", sort=list(user_posts_by_year["Person"].drop_duplicates())),
            color=alt.Color("Uses:Q", title="Uses"),
            tooltip=["Person:N", "Emoji Label:N", "Uses:Q"],
        )
        .properties(height=280, title="Emoji preferences by person")
    )
    mo.vstack(
        [
            mo.md("## Charts"),
            yearly_chart,
            monthly_chart,
            emoji_chart,
            emoji_heatmap,
        ],
        gap=1,
    )
    return emoji_chart, emoji_heatmap, monthly_chart, yearly_chart


@app.cell
def _(
    emoji_favorites,
    emoji_favorites_detail,
    emoji_usage_by_person_matrix,
    mo,
    render_table,
    reactions,
    thread_activity,
    top_emojis,
    top_channels,
    top_users,
    user_posts_by_month_matrix,
    user_posts_by_year_matrix,
):
    mo.accordion(
        {
            "Users": render_table(
                mo,
                top_users,
                label="Users",
            ),
            "User posts by year": render_table(
                mo,
                user_posts_by_year_matrix,
                label="User posts by year",
                page_size=24,
                pagination=True,
            ),
            "User posts by recent month": render_table(
                mo,
                user_posts_by_month_matrix,
                label="User posts by recent month",
                page_size=24,
                pagination=True,
            ),
            "Top channels": render_table(
                mo,
                top_channels,
                label="Top channels",
                wrapped_columns=["Channel"],
            ),
            "Reaction personalities": mo.vstack(
                [
                    render_table(
                        mo,
                        reactions,
                        label="Who reacts the most",
                    ),
                    render_table(
                        mo,
                        thread_activity,
                        label="Who starts threaded conversations",
                    ),
                    render_table(
                        mo,
                        top_emojis[["Preview", "Name", "Uses", "People", "Messages"]].to_dict("records"),
                        label="Top emojis",
                    ),
                    render_table(
                        mo,
                        emoji_favorites.to_dict("records"),
                        label="Favorite emojis by person",
                        pagination=False,
                        page_size=24,
                    ),
                    render_table(
                        mo,
                        emoji_favorites_detail[["Person", "Rank", "Preview", "Name", "Uses"]].to_dict("records"),
                        label="Favorite emoji counts by person",
                        page_size=24,
                    ),
                    render_table(
                        mo,
                        emoji_usage_by_person_matrix,
                        label="Emoji preferences matrix",
                        page_size=24,
                    ),
                ],
                gap=1,
            ),
        },
        multiple=True,
    )
    return


@app.cell
def _(message_cte, mo, params, reaction_cte, reaction_scope_params):
    param_preview = ", ".join(repr(value) for value in params) if params else "none"
    reaction_param_preview = (
        ", ".join(repr(value) for value in reaction_scope_params)
        if reaction_scope_params
        else "none"
    )
    mo.accordion(
        {
            "Advanced / query basis": mo.md(
                f"### Message scope\n```sql\n{message_cte.strip()}\n```\n\nMessage params: `{param_preview}`\n\n### Reaction scope\n```sql\n{reaction_cte.strip()}\n```\n\nReaction params: `{reaction_param_preview}`"
            )
        }
    )
    return


if __name__ == "__main__":
    app.run()
