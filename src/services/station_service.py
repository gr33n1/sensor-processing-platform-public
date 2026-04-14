from pathlib import Path

from src.repositories.sensor_repository import SensorRepository
from src.schemas import MetricsResponse, ProcessStationResponse

DB_PATH = Path(__file__).resolve().parents[2] / "sensor_data.db"


def process_station_data(
    station_id: str,
    start_time: str | None,
    end_time: str | None,
    resample_frequency: str,
) -> ProcessStationResponse:
    repository = SensorRepository(db_path=str(DB_PATH))
    readings_df = repository.get_station_readings(
        station_id=station_id,
        start_time=start_time,
        end_time=end_time,
    )

    return ProcessStationResponse(
        station_id=station_id,
        status="accepted",
        message=f"Loaded {len(readings_df)} rows. Processing not implemented yet.",
        start_time=start_time,
        end_time=end_time,
        resample_frequency=resample_frequency,
    )


def get_station_metrics_data(
    station_id: str,
    device_id: str | None,
    start_time: str | None,
    end_time: str | None,
) -> MetricsResponse:
    return MetricsResponse(
        station_id=station_id,
        device_id=device_id,
        start_time=start_time,
        end_time=end_time,
        metrics=[],
    )