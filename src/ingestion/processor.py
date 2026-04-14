from __future__ import annotations

import pandas as pd

from src.config import READINGS_TABLE
from src.ingestion.config import IngestionConfig, MissingDataStrategy
from src.ingestion.schema_loader import SchemaLoader


class SensorDataProcessor:
    GROUP_COLUMNS = ["station_id", "device_id"]

    def __init__(
        self,
        config: IngestionConfig,
        schema_loader: SchemaLoader,
    ) -> None:
        self.config = config
        self.numeric_columns = list(schema_loader.get_numeric_columns(READINGS_TABLE))

    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df.copy()

        processed_df = df.copy()

        processed_df["timestamp"] = pd.to_datetime(
            processed_df["timestamp"], utc=True, errors="coerce"
        )
        processed_df = processed_df.dropna(subset=["timestamp"])
        processed_df = processed_df.sort_values(["device_id", "timestamp"])

        self._validate_required_columns(processed_df)

        for column in self.numeric_columns:
            processed_df[column] = pd.to_numeric(processed_df[column], errors="coerce")

        if self.config.missing_data_strategy == MissingDataStrategy.DROP:
            processed_df = processed_df.dropna(subset=self.numeric_columns)
        elif self.config.missing_data_strategy == MissingDataStrategy.FILL:
            processed_df[self.numeric_columns] = processed_df[self.numeric_columns].fillna(
                self.config.fill_value
            )
        else:
            raise ValueError(
                f"Unsupported missing data strategy: {self.config.missing_data_strategy}"
            )

        if processed_df.empty:
            return processed_df

        return (
            processed_df.set_index("timestamp")
            .groupby(self.GROUP_COLUMNS)[self.numeric_columns]
            .resample(self.config.resample_frequency)
            .mean()
            .reset_index()
            .sort_values(["device_id", "timestamp"])
            .reset_index(drop=True)
        )

    def _validate_required_columns(self, df: pd.DataFrame) -> None:
        required_columns = {"timestamp", *self.GROUP_COLUMNS, *self.numeric_columns}
        missing_columns = required_columns.difference(df.columns)

        if missing_columns:
            missing_str = ", ".join(sorted(missing_columns))
            raise ValueError(f"Missing required columns for processing: {missing_str}")