from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import os


@dataclass(frozen=True)
class Settings:
    project_root: Path
    exports_dir: Path
    database_path: Path
    slack_token: str | None

    @classmethod
    def discover(cls) -> "Settings":
        root = Path(__file__).resolve().parents[2]
        env_values = load_dotenv(root / ".env")
        return cls(
            project_root=root,
            exports_dir=root / "exports",
            database_path=root / "slackalytics.duckdb",
            slack_token=os.environ.get("SLACK_TOKEN") or env_values.get("SLACK_TOKEN"),
        )


def load_dotenv(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}

    values: dict[str, str] = {}
    for raw_line in path.read_text().splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, raw_value = line.split("=", 1)
        key = key.strip()
        value = raw_value.strip()
        if not key:
            continue
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            value = value[1:-1]
        values[key] = value
    return values
