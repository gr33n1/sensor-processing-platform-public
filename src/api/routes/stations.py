
from fastapi import APIRouter, Query

from src.schemas import MetricsResponse, ProcessStationResponse
from src.services.station_service import (
    get_station_metrics_data,
    process_station_data,
)

router = APIRouter(prefix="/stations", tags=["stations"])


@router.post("/{station_id}/process", response_model=ProcessStationResponse)
def process_station(
    station_id: str,
    start_time: str | None = Query(default=None),
    end_time: str | None = Query(default=None),
    resample_frequency: str = Query(default="1h"),
) -> ProcessStationResponse:
    return process_station_data(
        station_id=station_id,
        start_time=start_time,
        end_time=end_time,
        resample_frequency=resample_frequency,
    )


@router.get("/{station_id}/metrics", response_model=MetricsResponse)
def get_station_metrics(
    station_id: str,
    device_id: str | None = Query(default=None),
    start_time: str | None = Query(default=None),
    end_time: str | None = Query(default=None),
) -> MetricsResponse:
    return get_station_metrics_data(
        station_id=station_id,
        device_id=device_id,
        start_time=start_time,
        end_time=end_time,
    )