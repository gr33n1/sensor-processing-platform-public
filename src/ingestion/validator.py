from __future__ import annotations

import pandas as pd

from src.config import READINGS_TABLE
from src.ingestion.models import DataQualityIssue, IssueSeverity, ProcessingReport
from src.ingestion.schema_loader import SchemaLoader


class SensorDataValidator:
    def __init__(self, schema_loader: SchemaLoader) -> None:
        self.schema_loader = schema_loader
        self.readings_schema = schema_loader.get_table_columns(READINGS_TABLE)

    def validate_readings(self, df: pd.DataFrame) -> ProcessingReport:
        issues: list[DataQualityIssue] = []

        self._validate_required_columns(df)

        missing_percent_by_column = self._compute_missing_percent(df)

        type_issues = self._validate_column_types(df)
        issues.extend(type_issues)

        out_of_range_counts, range_issues = self._validate_ranges(df)
        issues.extend(range_issues)

        return ProcessingReport(
            row_count_before=len(df),
            row_count_after=len(df),
            missing_percent_by_column=missing_percent_by_column,
            out_of_range_counts=out_of_range_counts,
            issues=issues,
        )

    def _validate_required_columns(self, df: pd.DataFrame) -> None:
        required_columns = self.schema_loader.get_required_columns(READINGS_TABLE)

        missing_required_columns = [
            column_name for column_name in required_columns if column_name not in df.columns
        ]

        if missing_required_columns:
            raise ValueError(
                f"Missing required columns: {', '.join(missing_required_columns)}"
            )

    def _compute_missing_percent(self, df: pd.DataFrame) -> dict[str, float]:
        result: dict[str, float] = {}

        for column_name in self.readings_schema:
            if column_name in df.columns:
                result[column_name] = float(df[column_name].isna().mean() * 100)

        return result

    def _validate_column_types(self, df: pd.DataFrame) -> list[DataQualityIssue]:
        issues: list[DataQualityIssue] = []

        for column_name in self.readings_schema:
            if column_name not in df.columns:
                continue

            expected_type = self.schema_loader.get_column_type(READINGS_TABLE, column_name)
            series = df[column_name]

            if expected_type == "datetime":
                parsed = pd.to_datetime(series, errors="coerce", utc=True)
                invalid_mask = series.notna() & parsed.isna()

            elif expected_type == "integer":
                numeric = pd.to_numeric(series, errors="coerce")
                invalid_mask = series.notna() & (numeric.isna() | (numeric % 1 != 0))

            elif expected_type == "float":
                numeric = pd.to_numeric(series, errors="coerce")
                invalid_mask = series.notna() & numeric.isna()

            elif expected_type == "string":
                invalid_mask = series.notna() & ~series.map(lambda x: isinstance(x, str))

            else:
                continue

            count = int(invalid_mask.sum())
            if count > 0:
                issues.append(
                    DataQualityIssue(
                        issue_type="invalid_type",
                        severity=IssueSeverity.WARNING,
                        message=(
                            f"Column {column_name} has {count} values "
                            f"that do not match expected type {expected_type}"
                        ),
                        column=column_name,
                        details={"count": count, "expected_type": expected_type},
                    )
                )

        return issues

    def _validate_ranges(
        self, df: pd.DataFrame
    ) -> tuple[dict[str, int], list[DataQualityIssue]]:
        out_of_range_counts: dict[str, int] = {}
        issues: list[DataQualityIssue] = []

        for column_name in self.readings_schema:
            if column_name not in df.columns:
                continue

            valid_range = self.schema_loader.get_valid_range(READINGS_TABLE, column_name)
            if valid_range is None:
                continue

            numeric_series = pd.to_numeric(df[column_name], errors="coerce")
            non_null_numeric = numeric_series.notna()

            min_value = valid_range.get("min")
            max_value = valid_range.get("max")

            mask = pd.Series(False, index=df.index)

            if min_value is not None:
                mask = mask | (non_null_numeric & (numeric_series < min_value))

            if max_value is not None:
                mask = mask | (non_null_numeric & (numeric_series > max_value))

            count = int(mask.sum())
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

        return out_of_range_counts, issues