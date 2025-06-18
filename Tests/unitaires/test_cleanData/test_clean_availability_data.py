from unittest.mock import MagicMock, patch
import pytest
from Jobs.cleanData import clean_availability_data

def test_clean_availability_data_calls_all_steps_in_order():
    mock_spark = MagicMock()

    # Mock des DataFrames intermédiaires
    raw_df = MagicMock()
    clean_stationcode_df = MagicMock()
    clean_geolocation_df = MagicMock()
    filtered_df = MagicMock()
    validated_df = MagicMock()
    converted_df = MagicMock()
    deduplicated_df = MagicMock()

    # Patch des fonctions utilisées dans clean_availability_data
    with patch("Jobs.cleanData.read_availability_data", return_value=raw_df) as mock_read, \
         patch("Jobs.cleanData.clean_stationcode", return_value=clean_stationcode_df) as mock_clean_stationcode, \
         patch("Jobs.cleanData.clean_geolocation", return_value=clean_geolocation_df) as mock_clean_geolocation, \
         patch("Jobs.cleanData.filter_numeric_columns", return_value=filtered_df) as mock_filter_numeric, \
         patch("Jobs.cleanData.validate_bike_consistency", return_value=validated_df) as mock_validate_bike, \
         patch("Jobs.cleanData.convert_timestamp", return_value=converted_df) as mock_convert_timestamp, \
         patch("Jobs.cleanData.deduplicate_availability", return_value=deduplicated_df) as mock_deduplicate, \
         patch("Jobs.cleanData.write_clean_availability") as mock_write:

        # Chaque fonction renvoie le DataFrame attendu par la suivante
        mock_clean_stationcode.return_value = clean_stationcode_df
        mock_clean_geolocation.return_value = clean_geolocation_df
        mock_filter_numeric.return_value = filtered_df
        mock_validate_bike.return_value = validated_df
        mock_convert_timestamp.return_value = converted_df
        mock_deduplicate.return_value = deduplicated_df

        # Mock du count() sur les DataFrames (pour print)
        raw_df.count.return_value = 100
        deduplicated_df.count.return_value = 80

        clean_availability_data(mock_spark)

        # Vérifier que les fonctions ont été appelées avec les bons objets
        mock_read.assert_called_once_with(mock_spark)
        mock_clean_stationcode.assert_called_once_with(raw_df)
        mock_clean_geolocation.assert_called_once_with(clean_stationcode_df)
        mock_filter_numeric.assert_called_once_with(clean_geolocation_df)
        mock_validate_bike.assert_called_once_with(filtered_df)
        mock_convert_timestamp.assert_called_once_with(validated_df)
        mock_deduplicate.assert_called_once_with(converted_df)
        mock_write.assert_called_once_with(deduplicated_df)

        # Vérifier que count a été appelé sur raw_df et sur deduplicated_df
        raw_df.count.assert_called_once()
        deduplicated_df.count.assert_called_once()

def test_clean_availability_data_raises_if_step_fails():
    mock_spark = MagicMock()
    raw_df = MagicMock()

    with patch("Jobs.cleanData.read_availability_data", return_value=raw_df), \
         patch("Jobs.cleanData.clean_stationcode", side_effect=Exception("Nettoyage stationcode échoué")):

        with pytest.raises(Exception, match="Nettoyage stationcode échoué"):
            clean_availability_data(mock_spark)
