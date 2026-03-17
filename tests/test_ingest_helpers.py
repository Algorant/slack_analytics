from slackalytics.ingest import build_message_key, parse_exported_through


def test_parse_exported_through() -> None:
    exported = parse_exported_through("3KG Slack export Mar 31 2019 - Mar 16 2026.zip")
    assert exported is not None
    assert exported.year == 2026
    assert exported.month == 3
    assert exported.day == 16


def test_build_message_key_prefers_channel_id() -> None:
    assert (
        build_message_key("C123", "general", "1710104382.246619", None, {})
        == "C123:1710104382.246619"
    )


def test_build_message_key_falls_back_to_channel_name() -> None:
    assert (
        build_message_key(None, "general", "1710104382.246619", None, {})
        == "general:1710104382.246619"
    )


def test_build_message_key_falls_back_to_client_msg_id() -> None:
    assert (
        build_message_key("C123", "general", None, "abc-123", {"text": "hi"})
        == "C123:client:abc-123"
    )
