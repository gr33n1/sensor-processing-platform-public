from __future__ import annotations

import pandas as pd

from src.ingestion.config import IngestionConfig, MissingDataStrategy


class SensorDataProcessor:
    NUMERIC_COLUMNS = [
        "discharge_pressure",
        "air_flow_rate",
        "power_consumption",
        "motor_speed",
        "discharge_temp",
    ]

    GROUP_COLUMNS = ["station_id", "device_id"]

    def __init__(self, config: IngestionConfig) -> None:
        self.config = config

    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df.copy()

        processed_df = df.copy()

        processed_df["timestamp"] = pd.to_datetime(processed_df["timestamp"], utc=True)
        processed_df = processed_df.sort_values(["device_id", "timestamp"])

        self._validate_required_columns(processed_df)

        if self.config.missing_data_strategy == MissingDataStrategy.DROP:
            processed_df = processed_df.dropna(subset=self.NUMERIC_COLUMNS)
        elif self.config.missing_data_strategy == MissingDataStrategy.FILL:
            processed_df[self.NUMERIC_COLUMNS] = processed_df[self.NUMERIC_COLUMNS].fillna(
                self.config.fill_value
            )
        else:
            raise ValueError(
                f"Unsupported missing data strategy: {self.config.missing_data_strategy}"
            )

        if processed_df.empty:
            return processed_df

        processed_df = (
            processed_df.set_index("timestamp")
            .groupby(self.GROUP_COLUMNS)[self.NUMERIC_COLUMNS]
            .resample(self.config.resample_frequency)
            .mean()
            .reset_index()
            .sort_values(["device_id", "timestamp"])
            .reset_index(drop=True)
        )

        return processed_df

    def _validate_required_columns(self, df: pd.DataFrame) -> None:
        required_columns = {"timestamp", *self.GROUP_COLUMNS, *self.NUMERIC_COLUMNS}
        missing_columns = required_columns.difference(df.columns)

        if missing_columns:
            missing_str = ", ".join(sorted(missing_columns))
            raise ValueError(f"Missing required columns for processing: {missing_str}")