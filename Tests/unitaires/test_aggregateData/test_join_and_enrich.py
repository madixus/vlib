from unittest.mock import MagicMock, patch
from Jobs.aggregateData import join_and_enrich
import pytest

# 🧪 Test 1 – Cas nominal : le join et le withColumn sont bien appelés
def test_join_and_enrich_success():
    avail_df = MagicMock()
    station_df = MagicMock()
    joined_df_mock = MagicMock()
    enriched_df_mock = MagicMock()

    # Simuler chaînage des méthodes Spark
    avail_df.join.return_value = joined_df_mock
    joined_df_mock.withColumn.return_value = enriched_df_mock

    with patch("Jobs.aggregateData.current_timestamp") as mock_timestamp:
        mock_timestamp.return_value = "timestamp_mock"

        result = join_and_enrich(avail_df, station_df)

        # Vérification des appels
        avail_df.join.assert_called_once_with(station_df, on="stationcode", how="left")
        joined_df_mock.withColumn.assert_called_once_with("aggregation_timestamp", "timestamp_mock")

        # Le résultat doit être l'objet enrichi
        assert result == enriched_df_mock

# 🧪 Test 2 – Erreur pendant le join
def test_join_and_enrich_join_error():
    avail_df = MagicMock()
    station_df = MagicMock()
    avail_df.join.side_effect = Exception("Erreur join")

    with pytest.raises(Exception, match="Erreur join"):
        join_and_enrich(avail_df, station_df)

# 🧪 Test 3 – Erreur pendant le withColumn
def test_join_and_enrich_with_column_error():
    avail_df = MagicMock()
    station_df = MagicMock()
    joined_df_mock = MagicMock()
    avail_df.join.return_value = joined_df_mock
    joined_df_mock.withColumn.side_effect = Exception("Erreur withColumn")

    with patch("Jobs.aggregateData.current_timestamp") as mock_timestamp:
        mock_timestamp.return_value = "timestamp_mock"

        with pytest.raises(Exception, match="Erreur withColumn"):
            join_and_enrich(avail_df, station_df)
