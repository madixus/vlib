from unittest.mock import MagicMock
from Jobs.loadDataPostgresql import create_dim_station

def test_create_dim_station_calls_select_and_dropduplicates():
    # Mock du DataFrame Spark
    df_mock = MagicMock()
    df_selected = MagicMock()
    df_result = MagicMock()

    # Quand on appelle select, on retourne df_selected (chainage)
    df_mock.select.return_value = df_selected
    # Quand on appelle dropDuplicates sur df_selected, on retourne df_result
    df_selected.dropDuplicates.return_value = df_result

    # Appel de la fonction avec notre mock
    result = create_dim_station(df_mock)

    # Vérifier que select est appelé avec les bonnes colonnes
    df_mock.select.assert_called_once_with(
        "stationcode", "name", "lat", "lon", "arrondissement", "capacity", "station_opening_hours"
    )
    # Vérifier que dropDuplicates est appelé avec la bonne colonne
    df_selected.dropDuplicates.assert_called_once_with(["stationcode"])

    # Le résultat de la fonction doit être ce que dropDuplicates retourne
    assert result == df_result
