from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class MissingDataStrategy(str, Enum):
    DROP = "drop"
    FILL = "fill"


@dataclass(slots=True)
class IngestionConfig:
    resample_frequency: str = "1h"
    missing_data_strategy: MissingDataStrategy = MissingDataStrategy.DROP
    fill_value: float = 0.0