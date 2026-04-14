from __future__ import annotations
from pathlib import Path
import pandas as pd
import pytest

from src.ingestion import SchemaLoader
from src.ingestion.config import IngestionConfig, MissingDataStrategy
from src.ingestion.processor import SensorDataProcessor


def make_processor(
    *,
    resample_frequency: str = "1h",
    missing_data_strategy: MissingDataStrategy = MissingDataStrategy.DROP,
    fill_value: float = 0.0,
) -> SensorDataProcessor:
    schema_loader = SchemaLoader(Path(__file__).resolve().parents[1] / "sensor_schema.json")
    return SensorDataProcessor(
        IngestionConfig(
            resample_frequency=resample_frequency,
            missing_data_strategy=missing_data_strategy,
            fill_value=fill_value,
        ),
        schema_loader=schema_loader,
    )


def make_row(
    *,
    timestamp: str,
    station_id: str = "station_1",
    device_id: str = "device_1",
    discharge_pressure: float | None = 8.0,
    air_flow_rate: float | None = 100.0,
    power_consumption: float | None = 50.0,
    motor_speed: int | None = 1400,
    discharge_temp: float | None = 60.0,
) -> dict:
    return {
        "timestamp": timestamp,
        "station_id": station_id,
        "device_id": device_id,
        "discharge_pressure": discharge_pressure,
        "air_flow_rate": air_flow_rate,
        "power_consumption": power_consumption,
        "motor_speed": motor_speed,
        "discharge_temp": discharge_temp,
    }


def make_processor_df(rows: list[dict]) -> pd.DataFrame:
    return pd.DataFrame(rows)


def test_processor_resamples_rows_within_same_hour() -> None:
    df = make_processor_df(
        [
            make_row(timestamp="2024-02-01T00:00:00+00:00"),
            make_row(
                timestamp="2024-02-01T00:30:00+00:00",
                discharge_pressure=10.0,
                air_flow_rate=120.0,
                power_consumption=70.0,
                motor_speed=1600,
                discharge_temp=70.0,
            ),
        ]
    )

    result = make_processor().process(df)

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
            make_row(timestamp="2024-02-01T00:00:00+00:00"),
            make_row(
                timestamp="2024-02-01T00:30:00+00:00",
                discharge_pressure=None,
                air_flow_rate=120.0,
                power_consumption=70.0,
                motor_speed=1600,
                discharge_temp=70.0,
            ),
        ]
    )

    result = make_processor().process(df)

    assert len(result) == 1
    assert result.loc[0, "discharge_pressure"] == 8.0
    assert result.loc[0, "air_flow_rate"] == 100.0


def test_processor_fill_strategy_fills_missing_numeric_values_before_resampling() -> None:
    df = make_processor_df(
        [
            make_row(timestamp="2024-02-01T00:00:00+00:00"),
            make_row(
                timestamp="2024-02-01T00:30:00+00:00",
                discharge_pressure=None,
                air_flow_rate=120.0,
                power_consumption=70.0,
                motor_speed=1600,
                discharge_temp=70.0,
            ),
        ]
    )

    result = make_processor(
        missing_data_strategy=MissingDataStrategy.FILL,
        fill_value=0.0,
    ).process(df)

    assert len(result) == 1
    assert result.loc[0, "discharge_pressure"] == 4.0
    assert result.loc[0, "air_flow_rate"] == 110.0


def test_processor_keeps_devices_separate_during_resampling() -> None:
    df = make_processor_df(
        [
            make_row(timestamp="2024-02-01T00:00:00+00:00", device_id="device_1"),
            make_row(
                timestamp="2024-02-01T00:10:00+00:00",
                device_id="device_2",
                discharge_pressure=12.0,
                air_flow_rate=200.0,
                power_consumption=90.0,
                motor_speed=1800,
                discharge_temp=80.0,
            ),
        ]
    )

    result = make_processor().process(df)

    assert len(result) == 2
    assert set(result["device_id"]) == {"device_1", "device_2"}


def test_processor_parses_timestamp_as_utc_datetime() -> None:
    df = make_processor_df(
        [make_row(timestamp="2024-02-01T00:00:00+00:00")]
    )

    result = make_processor().process(df)

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

    with pytest.raises(ValueError, match="Missing required columns for processing"):
        make_processor().process(df)