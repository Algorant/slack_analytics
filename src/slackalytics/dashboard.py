from __future__ import annotations

from slackalytics.people import PRIMARY_USERS, SYSTEM_USER_IDS


def canonical_label_sql(
    message_alias: str = "m",
    user_alias: str = "u",
    fallback_label_expr: str | None = None,
) -> str:
    fallback_label_expr = fallback_label_expr or f"{message_alias}.user_label"
    primary_cases = "\n".join(
        f"        when {message_alias}.user_id = '{user['user_id']}' then '{user['label']}'"
        for user in PRIMARY_USERS
    )
    return f"""
    case
{primary_cases}
        when {user_alias}.real_name is not null and trim({user_alias}.real_name) <> '' then {user_alias}.real_name
        when {user_alias}.name is not null and trim({user_alias}.name) <> '' then {user_alias}.name
        when {fallback_label_expr} is not null and trim({fallback_label_expr}) <> '' then {fallback_label_expr}
        when {message_alias}.user_id is not null and trim({message_alias}.user_id) <> '' then {message_alias}.user_id
        else 'Unknown'
    end
    """.strip()


def word_count_sql(text_expr: str) -> str:
    return f"""
    case
        when trim(coalesce({text_expr}, '')) = '' then 0
        else array_length(regexp_split_to_array(trim(coalesce({text_expr}, '')), '\\s+'))
    end
    """.strip()


SYSTEM_USER_ID_SQL = ", ".join(f"'{user_id}'" for user_id in SYSTEM_USER_IDS)
