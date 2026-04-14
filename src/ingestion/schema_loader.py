from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def load_schema(schema_path: str | Path) -> dict[str, Any]:
    path = Path(schema_path)

    if not path.exists():
        raise FileNotFoundError(f"Schema file not found: {path}")

    with path.open("r", encoding="utf-8") as file:
        schema = json.load(file)

    if not isinstance(schema, dict):
        raise ValueError("Schema must contain a JSON object")

    return schema