from unittest.mock import MagicMock, patch
from Jobs.aggregateData import get_latest_station_info
import pytest

# 🧪 Test 1 – Cas nominal : toutes les étapes sont bien appelées
def test_get_latest_station_info_success():
    station_df = MagicMock()
    withColumn_df = MagicMock()
    filtered_df = MagicMock()
    final_df = MagicMock()

    # Chaînage
    station_df.withColumn.return_value = withColumn_df
    withColumn_df.filter.return_value = filtered_df
    filtered_df.drop.return_value = final_df

    with patch("Jobs.aggregateData.Window") as mock_window, \
         patch("Jobs.aggregateData.col") as mock_col, \
         patch("Jobs.aggregateData.row_number") as mock_row_number:

        # Simule la fenêtre
        mock_partition = MagicMock()
        mock_window.partitionBy.return_value = mock_partition
        mock_partition.orderBy.return_value = "window_spec"

        # Simule row_number().over(...)
        mock_row_number.return_value.over.return_value = "row_number_col"

        # Simule col("rn") == 1
        rn_col = MagicMock()
        mock_col.return_value = rn_col
        rn_col.__eq__.return_value = "filter_condition"

        result = get_latest_station_info(station_df)

        # Vérifie les appels
        mock_window.partitionBy.assert_called_once_with("stationcode")
        mock_partition.orderBy.assert_called_once()
        mock_col.assert_any_call("timestamp")
        mock_col.assert_any_call("rn")
        mock_row_number.return_value.over.assert_called_once()

        station_df.withColumn.assert_called_once_with("rn", "row_number_col")
        withColumn_df.filter.assert_called_once_with("filter_condition")
        filtered_df.drop.assert_called_once_with("rn", "timestamp")

        # Vérifie la sortie
        assert result == final_df

# 🧪 Test 2 – Erreur lors de l'application de row_number
def test_get_latest_station_info_row_number_error():
    station_df = MagicMock()

    with patch("Jobs.aggregateData.Window") as mock_window, \
         patch("Jobs.aggregateData.col"), \
         patch("Jobs.aggregateData.row_number", side_effect=Exception("Erreur row_number")):

        with pytest.raises(Exception, match="Erreur row_number"):
            get_latest_station_info(station_df)

# 🧪 Test 3 – Erreur si filter échoue
def test_get_latest_station_info_filter_error():
    station_df = MagicMock()
    withColumn_df = MagicMock()
    station_df.withColumn.return_value = withColumn_df

    withColumn_df.filter.side_effect = Exception("Erreur filter")

    with patch("Jobs.aggregateData.Window") as mock_window, \
         patch("Jobs.aggregateData.col") as mock_col, \
         patch("Jobs.aggregateData.row_number") as mock_row_number:

        mock_partition = MagicMock()
        mock_window.partitionBy.return_value = mock_partition
        mock_partition.orderBy.return_value = "window_spec"

        mock_row_number.return_value.over.return_value = "row_number_col"
        rn_col = MagicMock()
        mock_col.return_value = rn_col
        rn_col.__eq__.return_value = "filter_condition"

        with pytest.raises(Exception, match="Erreur filter"):
            get_latest_station_info(station_df)
