import pytest
from unittest.mock import MagicMock, patch
from pyspark.sql import Row
from pyspark.sql.functions import col
from Jobs.loadDataPostgresql import filter_existing_timestamps

def make_row(event_ts):
    # Simule un Row Spark avec __getitem__ supporté
    row = MagicMock()
    row.__getitem__.side_effect = lambda k: event_ts if k == "event_ts" else None
    return row

@patch("Jobs.loadDataPostgresql.col")  # Patch col pour éviter d'utiliser la vraie fonction spark
def test_filter_existing_timestamps_success(mock_col):
    # Création de faux rows renvoyés par collect()
    mock_rows = [make_row("2024-06-01T12:00:00"), make_row("2024-06-01T12:05:00")]

    spark = MagicMock()
    dim_time_pg_mock = MagicMock()
    dim_time_pg_mock.collect.return_value = mock_rows
    spark.read.jdbc.return_value.select.return_value = dim_time_pg_mock

    df_raw = MagicMock()
    df_filtered = MagicMock()
    df_raw.filter.return_value = df_filtered
    df_filtered.count.return_value = 5

    # Le patch col("event_ts") renvoie une MagicMock (simplifie le test)
    mock_col.return_value = MagicMock()

    result = filter_existing_timestamps(spark, df_raw, "jdbc-url", {"user": "x", "password": "y"})

    spark.read.jdbc.assert_called_once()
    df_raw.filter.assert_called_once()
    df_filtered.count.assert_called_once()
    assert result == df_filtered

@patch("Jobs.loadDataPostgresql.col")
def test_filter_existing_timestamps_pg_empty(mock_col):
    spark = MagicMock()
    dim_time_pg_mock = MagicMock()
    dim_time_pg_mock.collect.return_value = []
    spark.read.jdbc.return_value.select.return_value = dim_time_pg_mock

    df_raw = MagicMock()
    df_filtered = MagicMock()
    df_raw.filter.return_value = df_filtered
    df_filtered.count.return_value = 10

    mock_col.return_value = MagicMock()

    result = filter_existing_timestamps(spark, df_raw, "jdbc-url", {"user": "x", "password": "y"})

    df_raw.filter.assert_called_once()
    assert result == df_filtered

def test_filter_existing_timestamps_pg_error():
    spark = MagicMock()
    spark.read.jdbc.side_effect = Exception("DB error")

    df_raw = MagicMock()

    result = filter_existing_timestamps(spark, df_raw, "jdbc-url", {"user": "x", "password": "y"})

    assert result == df_raw
