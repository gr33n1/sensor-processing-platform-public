from __future__ import annotations

import json
from pathlib import Path
from typing import Any

class SchemaLoader:
    def __init__(self, schema_path: str | Path) -> None:
        self.schema_path = Path(schema_path)
        self._schema = self._load_schema()

    def _load_schema(self) -> dict[str, Any]:
        if not self.schema_path.exists():
            raise FileNotFoundError(f"Schema file not found: {self.schema_path}")

        with open(self.schema_path, "r", encoding="utf-8") as f:
            return json.load(f)

    @property
    def schema(self) -> dict[str, Any]:
        return self._schema

    def get_table_schema(self, table_name: str) -> dict[str, Any]:
        try:
            return self._schema["tables"][table_name]
        except KeyError as exc:
            raise KeyError(f"Table {table_name!r} not found in schema") from exc

    def get_table_columns(self, table_name: str) -> dict[str, Any]:
        table_schema = self.get_table_schema(table_name)

        try:
            return table_schema["columns"]
        except KeyError as exc:
            raise KeyError(f"Table {table_name!r} does not define columns") from exc

    def get_required_columns(self, table_name: str) -> tuple[str, ...]:
        columns = self.get_table_columns(table_name)
        return tuple(
            column_name
            for column_name, column_schema in columns.items()
            if column_schema.get("required", False)
        )

    def get_numeric_columns(self, table_name: str) -> tuple[str, ...]:
        columns = self.get_table_columns(table_name)
        return tuple(
            column_name
            for column_name, column_schema in columns.items()
            if column_schema.get("type") in {"float", "integer"}
        )

    def get_column_schema(self, table_name: str, column_name: str) -> dict[str, Any]:
        columns = self.get_table_columns(table_name)

        try:
            return columns[column_name]
        except KeyError as exc:
            raise KeyError(
                f"Column {column_name!r} not found in table {table_name!r}"
            ) from exc

    def get_column_type(self, table_name: str, column_name: str) -> str | None:
        return self.get_column_schema(table_name, column_name).get("type")

    def get_valid_range(
        self, table_name: str, column_name: str
    ) -> dict[str, Any] | None:
        return self.get_column_schema(table_name, column_name).get("valid_range")