from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd
import pytest

from src.ingestion.schema_loader import load_schema
from src.ingestion.validator import SensorDataValidator

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCHEMA_PATH = PROJECT_ROOT / "sensor_schema.json"

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


@pytest.fixture
def schema() -> dict[str, Any]:
    return load_schema(SCHEMA_PATH)


@pytest.fixture
def validator(schema: dict[str, Any]) -> SensorDataValidator:
    return SensorDataValidator(schema)


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


def test_validator_raises_when_required_column_is_missing(validator: SensorDataValidator) -> None:
    df = make_valid_readings_df().drop(columns=["station_id"])

    with pytest.raises(ValueError, match="Missing required columns: station_id"):
        validator.validate_readings(df)


def test_validator_reports_missing_percentage_correctly(validator: SensorDataValidator) -> None:
    df = pd.DataFrame(
        [
            {
                "timestamp": "2024-02-01T00:00:00+00:00",
                "station_id": "station_1",
                "device_id": "device_1",
                "discharge_pressure": 8.5,
                "air_flow_rate": 120.0,
                "power_consumption": 55.0,
                "motor_speed": 1500,
                "discharge_temp": 65.0,
            },
            {
                "timestamp": "2024-02-01T01:00:00+00:00",
                "station_id": "station_1",
                "device_id": "device_1",
                "discharge_pressure": None,
                "air_flow_rate": 130.0,
                "power_consumption": 56.0,
                "motor_speed": 1510,
                "discharge_temp": 66.0,
            },
        ]
    )

    report = validator.validate_readings(df)

    assert report.missing_percent_by_column["discharge_pressure"] == 50.0


def test_validator_detects_value_below_min_range(validator: SensorDataValidator) -> None:
    df = make_valid_readings_df(discharge_pressure=-1.0)

    report = validator.validate_readings(df)

    assert report.out_of_range_counts["discharge_pressure"] == 1
    assert any(
        issue.column == "discharge_pressure" and issue.issue_type == "out_of_range"
        for issue in report.issues
    )


def test_validator_detects_value_above_max_range(validator: SensorDataValidator) -> None:
    df = make_valid_readings_df(air_flow_rate=999.0)

    report = validator.validate_readings(df)

    assert report.out_of_range_counts["air_flow_rate"] == 1
    assert any(
        issue.column == "air_flow_rate" and issue.issue_type == "out_of_range"
        for issue in report.issues
    )


def test_validator_does_not_count_null_as_out_of_range(validator: SensorDataValidator) -> None:
    df = make_valid_readings_df(discharge_pressure=None)

    report = validator.validate_readings(df)

    assert report.missing_percent_by_column["discharge_pressure"] == 100.0
    assert report.out_of_range_counts["discharge_pressure"] == 0


def test_validator_skips_non_numeric_columns_for_range_checks(validator: SensorDataValidator) -> None:
    df = make_valid_readings_df()

    report = validator.validate_readings(df)

    assert "timestamp" not in report.out_of_range_counts
    assert "station_id" not in report.out_of_range_counts
    assert "device_id" not in report.out_of_range_counts