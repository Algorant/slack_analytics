from pathlib import Path

from slackalytics.config import Settings, load_dotenv


def test_load_dotenv_parses_values_and_ignores_comments(tmp_path: Path) -> None:
    dotenv = tmp_path / ".env"
    dotenv.write_text(
        "\n".join(
            [
                "# comment",
                "SLACK_TOKEN = xoxb-plain-token",
                "QUOTED_DOUBLE = \"double value\"",
                "QUOTED_SINGLE = 'single value'",
                "",
            ]
        )
    )

    values = load_dotenv(dotenv)

    assert values["SLACK_TOKEN"] == "xoxb-plain-token"
    assert values["QUOTED_DOUBLE"] == "double value"
    assert values["QUOTED_SINGLE"] == "single value"


def test_settings_discover_prefers_process_env(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("SLACK_TOKEN", "process-token")
    src_dir = tmp_path / "src" / "slackalytics"
    src_dir.mkdir(parents=True)
    (tmp_path / ".env").write_text("SLACK_TOKEN=dotenv-token\n")

    config_file = src_dir / "config.py"
    config_file.write_text("# placeholder\n")
    monkeypatch.setattr("slackalytics.config.Path.resolve", lambda self: config_file)

    settings = Settings.discover()

    assert settings.slack_token == "process-token"


def test_settings_discover_reads_dotenv(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.delenv("SLACK_TOKEN", raising=False)
    src_dir = tmp_path / "src" / "slackalytics"
    src_dir.mkdir(parents=True)
    (tmp_path / ".env").write_text("SLACK_TOKEN=dotenv-token\n")

    config_file = src_dir / "config.py"
    config_file.write_text("# placeholder\n")
    monkeypatch.setattr("slackalytics.config.Path.resolve", lambda self: config_file)

    settings = Settings.discover()

    assert settings.slack_token == "dotenv-token"
