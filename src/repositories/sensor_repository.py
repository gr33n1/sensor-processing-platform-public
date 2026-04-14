from __future__ import annotations

import logging
import sqlite3

import pandas as pd

logger = logging.getLogger(__name__)


class SensorRepository:
    def __init__(self, db_path: str) -> None:
        self.db_path = db_path

    def get_station_readings(
        self,
        station_id: str,
        start_time: str | None = None,
        end_time: str | None = None,
    ) -> pd.DataFrame:
        query = """
            SELECT *
            FROM sensor_readings
            WHERE station_id = ?
        """
        params: list[object] = [station_id]

        if start_time is not None:
            query += " AND timestamp >= ?"
            params.append(start_time)

        if end_time is not None:
            query += " AND timestamp <= ?"
            params.append(end_time)

        query += " ORDER BY timestamp ASC"

        with sqlite3.connect(self.db_path) as conn:
            df = pd.read_sql_query(query, conn, params=params)

        logger.info("Retrieved %s rows for station_id=%s", len(df), station_id)
        return df

    def get_station_metadata(self, station_id: str) -> pd.DataFrame:
        query = """
            SELECT *
            FROM station_metadata
            WHERE station_id = ?
        """

        params = [station_id]

        logger.debug("Executing get_station_metadata query: %s | params=%s", query, params)

        with sqlite3.connect(self.db_path) as conn:
            df = pd.read_sql_query(query, conn, params=params)

        logger.info("Retrieved %s metadata rows for station_id=%s", len(df), station_id)
        return df