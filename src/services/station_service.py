from pathlib import Path
from fastapi import HTTPException, status

from src.ingestion.config import IngestionConfig, MissingDataStrategy
from src.ingestion.processor import SensorDataProcessor
from src.ingestion.schema_loader import load_schema
from src.ingestion.validator import SensorDataValidator
from src.repositories.sensor_repository import SensorRepository
from src.schemas import MetricsResponse, ProcessStationResponse

DB_PATH = Path(__file__).resolve().parents[2] / "sensor_data.db"
SCHEMA_PATH = Path(__file__).resolve().parents[2] / "sensor_schema.json"

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
    if readings_df.empty:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No sensor readings found for station_id={station_id}",
        )

    schema = load_schema(SCHEMA_PATH)
    validator = SensorDataValidator(schema=schema)
    report = validator.validate_readings(readings_df)

    processor_config = IngestionConfig(
        resample_frequency=resample_frequency,
        missing_data_strategy=MissingDataStrategy.DROP,
        fill_value=0.0,
    )
    processor = SensorDataProcessor(config=processor_config)
    processed_df = processor.process(readings_df)

    return ProcessStationResponse(
        station_id=station_id,
        status="accepted",
        message=(
            f"Loaded {len(readings_df)} rows, "
            f"processed into {len(processed_df)} rows, "
            f"found {len(report.issues)} validation issues."
        ),
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