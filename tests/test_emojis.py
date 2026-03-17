from slackalytics.emojis import resolve_emoji_name, standard_emoji_for_name


def test_standard_emoji_for_name_resolves_alias() -> None:
    assert standard_emoji_for_name("joy") == "😂"


def test_standard_emoji_for_name_preserves_skin_tone_suffix() -> None:
    assert standard_emoji_for_name("+1::skin-tone-2") == "👍🏻"


def test_resolve_emoji_name_follows_custom_alias_chain() -> None:
    row = resolve_emoji_name(
        "party-time",
        {
            "party-time": "alias:party-parrot",
            "party-parrot": "https://example.com/party-parrot.png",
        },
    )
    assert row.emoji_kind == "custom_image"
    assert row.image_url == "https://example.com/party-parrot.png"
    assert row.alias_target == "party-parrot"
    assert row.display_value == ":party-parrot: party-parrot"


def test_resolve_emoji_name_falls_back_for_unknown_custom_name() -> None:
    row = resolve_emoji_name("yeschad", {})
    assert row.emoji_kind == "unknown"
    assert row.display_value == ":yeschad: yeschad"
    assert row.unicode_glyph is None
