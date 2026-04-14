from __future__ import annotations

from typing import Any

import pandas as pd

from src.ingestion.models import DataQualityIssue, IssueSeverity, ProcessingReport


class SensorDataValidator:
    def __init__(self, schema: dict[str, Any]) -> None:
        self.schema = schema

    def validate_readings(self, df: pd.DataFrame) -> ProcessingReport:
        readings_schema = self.schema["tables"]["sensor_readings"]["columns"]
        issues: list[DataQualityIssue] = []

        required_columns = [
            column_name
            for column_name, column_schema in readings_schema.items()
            if column_schema.get("required", False)
        ]

        missing_required_columns = [
            column_name for column_name in required_columns if column_name not in df.columns
        ]

        if missing_required_columns:
            raise ValueError(
                f"Missing required columns: {', '.join(missing_required_columns)}"
            )

        missing_percent_by_column: dict[str, float] = {}
        for column_name in df.columns:
            missing_percent_by_column[column_name] = float(df[column_name].isna().mean() * 100)

        out_of_range_counts: dict[str, int] = {}
        for column_name, column_schema in readings_schema.items():
            if column_name not in df.columns:
                continue

            valid_range = column_schema.get("valid_range")
            if valid_range is None:
                continue

            series = df[column_name]
            if not pd.api.types.is_numeric_dtype(series):
                out_of_range_counts[column_name] = 0
                continue

            mask = pd.Series(False, index=series.index)

            min_value = valid_range.get("min")
            max_value = valid_range.get("max")

            if min_value is not None:
                mask = mask | (series < min_value)

            if max_value is not None:
                mask = mask | (series > max_value)

            count = int(mask.fillna(False).sum())
            out_of_range_counts[column_name] = count

            if count > 0:
                issues.append(
                    DataQualityIssue(
                        issue_type="out_of_range",
                        severity=IssueSeverity.WARNING,
                        message=f"Column {column_name} has {count} out-of-range values",
                        column=column_name,
                        details={"count": count},
                    )
                )

        return ProcessingReport(
            row_count_before=len(df),
            row_count_after=len(df),
            missing_percent_by_column=missing_percent_by_column,
            out_of_range_counts=out_of_range_counts,
            issues=issues,
        )