
from __future__ import annotations

from pydantic import BaseModel


class ProcessStationResponse(BaseModel):
    station_id: str
    status: str
    message: str
    start_time: str | None = None
    end_time: str | None = None
    resample_frequency: str


class MetricItem(BaseModel):
    metric_name: str
    value: float
    unit: str | None = None
    device_id: str | None = None


class MetricsResponse(BaseModel):
    station_id: str
    device_id: str | None = None
    start_time: str | None = None
    end_time: str | None = None
    metrics: list[MetricItem]