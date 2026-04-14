from __future__ import annotations

import json
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DB_PATH = PROJECT_ROOT / "sensor_data.db"
SCHEMA_PATH = PROJECT_ROOT / "sensor_schema.json"

READINGS_TABLE = "sensor_readings"
METADATA_TABLE = "station_metadata"


def _load_schema(schema_path: Path) -> dict[str, Any]:
    with open(schema_path, "r", encoding="utf-8") as f:
        return json.load(f)


def _get_table_columns(schema: dict[str, Any], table_name: str) -> dict[str, Any]:
    return schema["tables"][table_name]["columns"]


_SCHEMA = _load_schema(SCHEMA_PATH)
_READINGS_COLUMNS = _get_table_columns(_SCHEMA, READINGS_TABLE)

READINGS_REQUIRED_COLUMNS = tuple(
    column_name
    for column_name, column_schema in _READINGS_COLUMNS.items()
    if column_schema.get("required", False)
)

READINGS_NUMERIC_COLUMNS = tuple(
    column_name
    for column_name, column_schema in _READINGS_COLUMNS.items()
    if column_schema.get("type") in {"float", "integer"}
)