from __future__ import annotations

import pandas as pd

from src.ingestion.config import IngestionConfig, MissingDataStrategy
from src.ingestion.processor import SensorDataProcessor


def make_processor_df(rows: list[dict]) -> pd.DataFrame:
    return pd.DataFrame(rows)


def test_processor_resamples_rows_within_same_hour() -> None:
    df = make_processor_df(
        [
            {
                "timestamp": "2024-02-01T00:00:00+00:00",
                "station_id": "station_1",
                "device_id": "device_1",
                "discharge_pressure": 8.0,
                "air_flow_rate": 100.0,
                "power_consumption": 50.0,
                "motor_speed": 1400,
                "discharge_temp": 60.0,
            },
            {
                "timestamp": "2024-02-01T00:30:00+00:00",
                "station_id": "station_1",
                "device_id": "device_1",
                "discharge_pressure": 10.0,
                "air_flow_rate": 120.0,
                "power_consumption": 70.0,
                "motor_speed": 1600,
                "discharge_temp": 70.0,
            },
        ]
    )

    processor = SensorDataProcessor(
        IngestionConfig(
            resample_frequency="1h",
            missing_data_strategy=MissingDataStrategy.DROP,
        )
    )

    result = processor.process(df)

    assert len(result) == 1
    assert result.loc[0, "station_id"] == "station_1"
    assert result.loc[0, "device_id"] == "device_1"
    assert result.loc[0, "discharge_pressure"] == 9.0
    assert result.loc[0, "air_flow_rate"] == 110.0
    assert result.loc[0, "power_consumption"] == 60.0
    assert result.loc[0, "motor_speed"] == 1500.0
    assert result.loc[0, "discharge_temp"] == 65.0


def test_processor_drop_strategy_removes_rows_with_missing_numeric_values() -> None:
    df = make_processor_df(
        [
            {
                "timestamp": "2024-02-01T00:00:00+00:00",
                "station_id": "station_1",
                "device_id": "device_1",
                "discharge_pressure": 8.0,
                "air_flow_rate": 100.0,
                "power_consumption": 50.0,
                "motor_speed": 1400,
                "discharge_temp": 60.0,
            },
            {
                "timestamp": "2024-02-01T00:30:00+00:00",
                "station_id": "station_1",
                "device_id": "device_1",
                "discharge_pressure": None,
                "air_flow_rate": 120.0,
                "power_consumption": 70.0,
                "motor_speed": 1600,
                "discharge_temp": 70.0,
            },
        ]
    )

    processor = SensorDataProcessor(
        IngestionConfig(
            resample_frequency="1h",
            missing_data_strategy=MissingDataStrategy.DROP,
        )
    )

    result = processor.process(df)

    assert len(result) == 1
    assert result.loc[0, "discharge_pressure"] == 8.0
    assert result.loc[0, "air_flow_rate"] == 100.0


def test_processor_fill_strategy_fills_missing_numeric_values_before_resampling() -> None:
    df = make_processor_df(
        [
            {
                "timestamp": "2024-02-01T00:00:00+00:00",
                "station_id": "station_1",
                "device_id": "device_1",
                "discharge_pressure": 8.0,
                "air_flow_rate": 100.0,
                "power_consumption": 50.0,
                "motor_speed": 1400,
                "discharge_temp": 60.0,
            },
            {
                "timestamp": "2024-02-01T00:30:00+00:00",
                "station_id": "station_1",
                "device_id": "device_1",
                "discharge_pressure": None,
                "air_flow_rate": 120.0,
                "power_consumption": 70.0,
                "motor_speed": 1600,
                "discharge_temp": 70.0,
            },
        ]
    )

    processor = SensorDataProcessor(
        IngestionConfig(
            resample_frequency="1h",
            missing_data_strategy=MissingDataStrategy.FILL,
            fill_value=0.0,
        )
    )

    result = processor.process(df)

    assert len(result) == 1
    assert result.loc[0, "discharge_pressure"] == 4.0
    assert result.loc[0, "air_flow_rate"] == 110.0


def test_processor_keeps_devices_separate_during_resampling() -> None:
    df = make_processor_df(
        [
            {
                "timestamp": "2024-02-01T00:00:00+00:00",
                "station_id": "station_1",
                "device_id": "device_1",
                "discharge_pressure": 8.0,
                "air_flow_rate": 100.0,
                "power_consumption": 50.0,
                "motor_speed": 1400,
                "discharge_temp": 60.0,
            },
            {
                "timestamp": "2024-02-01T00:10:00+00:00",
                "station_id": "station_1",
                "device_id": "device_2",
                "discharge_pressure": 12.0,
                "air_flow_rate": 200.0,
                "power_consumption": 90.0,
                "motor_speed": 1800,
                "discharge_temp": 80.0,
            },
        ]
    )

    processor = SensorDataProcessor(
        IngestionConfig(
            resample_frequency="1h",
            missing_data_strategy=MissingDataStrategy.DROP,
        )
    )

    result = processor.process(df)

    assert len(result) == 2
    assert set(result["device_id"]) == {"device_1", "device_2"}


def test_processor_parses_timestamp_as_utc_datetime() -> None:
    df = make_processor_df(
        [
            {
                "timestamp": "2024-02-01T00:00:00+00:00",
                "station_id": "station_1",
                "device_id": "device_1",
                "discharge_pressure": 8.0,
                "air_flow_rate": 100.0,
                "power_consumption": 50.0,
                "motor_speed": 1400,
                "discharge_temp": 60.0,
            }
        ]
    )

    processor = SensorDataProcessor(
        IngestionConfig(
            resample_frequency="1h",
            missing_data_strategy=MissingDataStrategy.DROP,
        )
    )

    result = processor.process(df)

    assert isinstance(result["timestamp"].dtype, pd.DatetimeTZDtype)
    assert str(result["timestamp"].dtype.tz) == "UTC"


def test_processor_raises_when_required_columns_are_missing() -> None:
    df = pd.DataFrame(
        [
            {
                "timestamp": "2024-02-01T00:00:00+00:00",
                "station_id": "station_1",
                "device_id": "device_1",
                "air_flow_rate": 100.0,
                "power_consumption": 50.0,
                "motor_speed": 1400,
                "discharge_temp": 60.0,
            }
        ]
    )

    processor = SensorDataProcessor(
        IngestionConfig(
            resample_frequency="1h",
            missing_data_strategy=MissingDataStrategy.DROP,
        )
    )

    try:
        processor.process(df)
        assert False, "Expected ValueError for missing required columns"
    except ValueError as exc:
        assert "Missing required columns for processing" in str(exc)
        assert "discharge_pressure" in str(exc)