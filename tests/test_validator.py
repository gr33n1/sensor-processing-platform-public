from __future__ import annotations

from typing import Any

import pandas as pd
import pytest

from src.config import READINGS_TABLE, SCHEMA_PATH
from src.ingestion.schema_loader import SchemaLoader
from src.ingestion.validator import SensorDataValidator


@pytest.fixture
def schema_loader() -> SchemaLoader:
    return SchemaLoader(SCHEMA_PATH)


@pytest.fixture
def validator(schema_loader: SchemaLoader) -> SensorDataValidator:
    return SensorDataValidator(schema_loader)


def make_valid_readings_df(**overrides: Any) -> pd.DataFrame:
    row = {
        "timestamp": "2024-02-01T00:00:00+00:00",
        "station_id": "station_1",
        "device_id": "device_1",
        "discharge_pressure": 8.5,
        "air_flow_rate": 120.0,
        "power_consumption": 55.0,
        "motor_speed": 1500,
        "discharge_temp": 65.0,
    }
    row.update(overrides)
    return pd.DataFrame([row])


def make_readings_df(rows: list[dict[str, Any]]) -> pd.DataFrame:
    return pd.concat(
        [make_valid_readings_df(**row) for row in rows],
        ignore_index=True,
    )


def assert_issue_exists(
    report,
    *,
    column: str,
    issue_type: str,
) -> None:
    assert any(
        issue.column == column and issue.issue_type == issue_type
        for issue in report.issues
    )


def assert_out_of_range_issue(
    report,
    *,
    column: str,
    expected_count: int = 1,
) -> None:
    assert report.out_of_range_counts[column] == expected_count
    assert_issue_exists(report, column=column, issue_type="out_of_range")


def test_validator_accepts_valid_dataframe(validator: SensorDataValidator) -> None:
    df = make_valid_readings_df()

    report = validator.validate_readings(df)

    assert report.row_count_before == 1
    assert report.row_count_after == 1
    assert report.issues == []
    assert report.out_of_range_counts["discharge_pressure"] == 0
    assert report.out_of_range_counts["air_flow_rate"] == 0
    assert report.out_of_range_counts["power_consumption"] == 0
    assert report.out_of_range_counts["motor_speed"] == 0
    assert report.out_of_range_counts["discharge_temp"] == 0


def test_validator_raises_when_required_column_is_missing(
    validator: SensorDataValidator,
) -> None:
    df = make_valid_readings_df().drop(columns=["station_id"])

    with pytest.raises(ValueError, match="Missing required columns: station_id"):
        validator.validate_readings(df)


def test_validator_reports_missing_percentage_correctly(
    validator: SensorDataValidator,
) -> None:
    df = make_readings_df(
        [
            {},
            {
                "timestamp": "2024-02-01T01:00:00+00:00",
                "discharge_pressure": None,
            },
        ]
    )

    report = validator.validate_readings(df)

    assert report.missing_percent_by_column["discharge_pressure"] == 50.0


@pytest.mark.parametrize(
    ("column", "value"),
    [
        ("discharge_pressure", -1.0),
        ("air_flow_rate", 999.0),
    ],
)
def test_validator_detects_out_of_range_values(
    validator: SensorDataValidator,
    column: str,
    value: Any,
) -> None:
    df = make_valid_readings_df(**{column: value})

    report = validator.validate_readings(df)

    assert_out_of_range_issue(report, column=column)


def test_validator_does_not_count_null_as_out_of_range(
    validator: SensorDataValidator,
) -> None:
    df = make_valid_readings_df(discharge_pressure=None)

    report = validator.validate_readings(df)

    assert report.missing_percent_by_column["discharge_pressure"] == 100.0
    assert report.out_of_range_counts["discharge_pressure"] == 0


@pytest.mark.parametrize(
    ("column", "value"),
    [
        ("timestamp", "not-a-datetime"),
        ("power_consumption", "bad-value"),
        ("motor_speed", 1500.5),
    ],
)

def test_validator_flags_invalid_types(
    validator: SensorDataValidator,
    column: str,
    value: Any,
) -> None:
    df = make_valid_readings_df(**{column: value})

    report = validator.validate_readings(df)

    assert_issue_exists(report, column=column, issue_type="invalid_type")


def test_validator_ignores_extra_unknown_columns(
    validator: SensorDataValidator,
) -> None:
    df = make_valid_readings_df(extra_column="unexpected")

    report = validator.validate_readings(df)

    assert report.row_count_before == 1
    assert report.row_count_after == 1


def test_validator_reports_only_schema_columns_in_missing_percent(
    validator: SensorDataValidator,
) -> None:
    df = make_valid_readings_df(extra_column="unexpected")

    report = validator.validate_readings(df)

    expected_columns = set(
        validator.schema_loader.get_table_columns(READINGS_TABLE).keys()
    )

    assert set(report.missing_percent_by_column.keys()) == expected_columns