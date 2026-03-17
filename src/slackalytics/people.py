from __future__ import annotations

PRIMARY_USERS = (
    {"label": "Ivan", "user_id": "UHH0R7TTP", "names": ("Ivan",)},
    {"label": "Al", "user_id": "UHEB1D3PA", "names": ("Al", "Alejandro")},
    {"label": "Stephen", "user_id": "UH81VL1TK", "names": ("Stephen",)},
    {"label": "Will", "user_id": "UHH7GF9S9", "names": ("Will",)},
    {"label": "Ben", "user_id": "UHHS66KU7", "names": ("Ben",)},
    {"label": "Michael", "user_id": "UHHK3HG48", "names": ("Michael",)},
    {"label": "Derek", "user_id": "UHAC3TPCZ", "names": ("Derek",)},
    {"label": "Roth", "user_id": "UHK1N52Q6", "names": ("Roth",)},
)

PRIMARY_USER_IDS = tuple(user["user_id"] for user in PRIMARY_USERS)
PRIMARY_USER_LABELS = tuple(user["label"] for user in PRIMARY_USERS)

SYSTEM_USER_IDS = (
    "USLACKBOT",
    "UN85LHRNK",  # scryfall
    "UUY8ZHGMA",  # Simple Poll
)

