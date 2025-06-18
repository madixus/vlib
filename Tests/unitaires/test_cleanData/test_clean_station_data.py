from unittest.mock import MagicMock, patch
import pytest
from Jobs.cleanData import clean_station_data

# === Mocks globaux partagés ===
mock_spark = MagicMock()
mock_df_initial = MagicMock()
mock_df_cleaned_1 = MagicMock()
mock_df_cleaned_2 = MagicMock()
mock_df_final = MagicMock()

mock_df_initial.count.side_effect = [100, 90]  # raw count puis cleaned count


# === Patch cible ===
@patch('Jobs.cleanData.read_station_data', return_value=mock_df_initial)
@patch('Jobs.cleanData.clean_stationcode', return_value=mock_df_cleaned_1)
@patch('Jobs.cleanData.filter_valid_geolocation', return_value=mock_df_cleaned_2)
@patch('Jobs.cleanData.deduplicate_stations', return_value=mock_df_final)
@patch('Jobs.cleanData.write_clean_stations')
def test_clean_station_data_happy_path(mock_write, mock_dedupe, mock_filter, mock_clean_code, mock_read):
    mock_df_final.count.return_value = 90

    clean_station_data(mock_spark)

    mock_read.assert_called_once_with(mock_spark)
    mock_clean_code.assert_called_once_with(mock_df_initial)
    mock_filter.assert_called_once_with(mock_df_cleaned_1)
    mock_dedupe.assert_called_once_with(mock_df_cleaned_2)
    mock_write.assert_called_once_with(mock_df_final)


@patch('Jobs.cleanData.read_station_data')
@patch('Jobs.cleanData.clean_stationcode')
@patch('Jobs.cleanData.filter_valid_geolocation')
@patch('Jobs.cleanData.deduplicate_stations')
@patch('Jobs.cleanData.write_clean_stations')
def test_clean_station_data_raises_on_write_error(mock_write, mock_dedupe, mock_filter, mock_clean_code, mock_read):
    mock_read.return_value = mock_df_initial
    mock_clean_code.return_value = mock_df_cleaned_1
    mock_filter.return_value = mock_df_cleaned_2
    mock_dedupe.return_value = mock_df_final
    mock_df_final.count.return_value = 90

    mock_write.side_effect = Exception("Erreur HDFS")

    with pytest.raises(Exception, match="Erreur HDFS"):
        clean_station_data(mock_spark)
