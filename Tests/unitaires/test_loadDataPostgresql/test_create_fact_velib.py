from unittest.mock import MagicMock, patch
import pytest
from Jobs.loadDataPostgresql import create_fact_velib

@patch("Jobs.loadDataPostgresql.logger")
def test_create_fact_velib_basic_flow(mock_logger):
    df_mock = MagicMock()
    df_selected = MagicMock()
    df_deduped = MagicMock()

    df_mock.select.return_value = df_selected
    df_selected.dropDuplicates.return_value = df_deduped
    df_deduped.count.return_value = 42

    result = create_fact_velib(df_mock)

    df_mock.select.assert_called_once_with(
        "event_ts", "stationcode", "num_bikes_available", "num_docks_available",
        "mechanical", "ebike", "is_installed", "is_renting", "is_returning",
        "capacity", "aggregation_timestamp"
    )
    df_selected.dropDuplicates.assert_called_once_with(["event_ts", "stationcode"])
    df_deduped.count.assert_called_once()

    mock_logger.info.assert_called_once_with("🛠 fact_velib générée : 42 lignes")

    assert result == df_deduped
