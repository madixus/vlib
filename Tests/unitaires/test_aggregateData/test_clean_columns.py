import pytest
from unittest.mock import MagicMock, patch, call
from Jobs.aggregateData import clean_columns


def make_mock_df():
    mock = MagicMock()
    mock.withColumn.return_value = mock
    return mock


def test_clean_columns_success():
    avail_df = make_mock_df()
    station_df = make_mock_df()

    with patch("Jobs.aggregateData.col") as mock_col, \
         patch("Jobs.aggregateData.trim") as mock_trim, \
         patch("Jobs.aggregateData.to_timestamp") as mock_to_timestamp:

        mock_col.return_value.cast.return_value = "casted_col"
        mock_trim.return_value = "trimmed_col"
        mock_to_timestamp.return_value = "timestamp_col"

        new_avail_df, new_station_df = clean_columns(avail_df, station_df)

        expected_calls = [
            call("stationcode"),
            call("stationcode")
        ]
        assert mock_col.call_args_list == expected_calls

        assert avail_df.withColumn.call_count == 2
        assert station_df.withColumn.call_count == 2
        assert new_avail_df == avail_df
        assert new_station_df == station_df

def test_clean_columns_raises_trim_error():
    avail_df = make_mock_df()
    station_df = make_mock_df()

    with patch("Jobs.aggregateData.col") as mock_col, \
         patch("Jobs.aggregateData.trim", side_effect=Exception("Erreur trim")), \
         patch("Jobs.aggregateData.to_timestamp"):

        mock_col.return_value.cast.return_value = "casted_col"

        with pytest.raises(Exception, match="Erreur trim"):
            clean_columns(avail_df, station_df)


def test_clean_columns_raises_timestamp_error():
    avail_df = make_mock_df()
    station_df = make_mock_df()

    with patch("Jobs.aggregateData.col") as mock_col, \
         patch("Jobs.aggregateData.trim") as mock_trim, \
         patch("Jobs.aggregateData.to_timestamp", side_effect=Exception("Erreur timestamp")):

        mock_col.return_value.cast.return_value = "casted_col"
        mock_trim.return_value = "trimmed_col"

        with pytest.raises(Exception, match="Erreur timestamp"):
            clean_columns(avail_df, station_df)
