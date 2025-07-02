from unittest.mock import MagicMock
from Jobs.aggregateData import select_useful_columns
import pytest

# Prépare un mock de DataFrame avec .select() chaînable
def make_mock_df():
    mock = MagicMock()
    mock.select.return_value = mock
    return mock

# 🧪 Test 1 – Sélection normale des colonnes
def test_select_useful_columns_success():
    avail_df = make_mock_df()
    station_df = make_mock_df()

    new_avail_df, new_station_df = select_useful_columns(avail_df, station_df)

    # Vérifie que les bons noms de colonnes sont passés à .select()
    avail_df.select.assert_called_once_with(
        "stationcode", "event_ts",
        "num_bikes_available", "num_docks_available",
        "mechanical", "ebike",
        "is_installed", "is_renting", "is_returning",
        "last_reported", "arrondissement"
    )

    station_df.select.assert_called_once_with(
        "stationcode", "timestamp",
        "name", "capacity", "lat", "lon", "station_opening_hours"
    )

    # Vérifie que les bons objets sont retournés
    assert new_avail_df == avail_df
    assert new_station_df == station_df

# 🧪 Test 2 – Erreur si une colonne manque dans avail_df
def test_select_useful_columns_missing_column_in_avail_df():
    avail_df = make_mock_df()
    station_df = make_mock_df()

    avail_df.select.side_effect = Exception("Column not found: 'ebike'")

    with pytest.raises(Exception, match="Column not found: 'ebike'"):
        select_useful_columns(avail_df, station_df)

# 🧪 Test 3 – Erreur si une colonne manque dans station_df
def test_select_useful_columns_missing_column_in_station_df():
    avail_df = make_mock_df()
    station_df = make_mock_df()

    station_df.select.side_effect = Exception("Column not found: 'lat'")

    with pytest.raises(Exception, match="Column not found: 'lat'"):
        select_useful_columns(avail_df, station_df)
