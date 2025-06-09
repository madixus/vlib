from unittest.mock import patch, MagicMock
from Jobs.getStationData import run_station_job

# 🎯 Cas 1 : exécution complète sans erreurs
@patch("Jobs.getStationData.write_to_hdfs")
@patch("Jobs.getStationData.SparkSession")
@patch("Jobs.getStationData.get_velib_station_schema")
@patch("Jobs.getStationData.transform_velib_data")
@patch("Jobs.getStationData.fetch_velib_data")
def test_run_station_job_success(
    mock_fetch,
    mock_transform,
    mock_schema,
    mock_spark_session,
    mock_write
):
    # 🔧 Configuration des mocks
    mock_fetch.return_value = [{"stationcode": "123"}]
    mock_transform.return_value = [{"stationcode": "123", "name": "Test", "capacity": 10, "lon": 2.3, "lat": 48.8, "station_opening_hours": "24/7", "timestamp": "2023-01-01T00:00:00"}]
    mock_schema.return_value = MagicMock()

    # ⚙️ Mock SparkSession + DataFrame
    mock_spark = MagicMock()
    mock_df = MagicMock()
    mock_spark.createDataFrame.return_value = mock_df
    mock_spark_session.builder.appName.return_value.getOrCreate.return_value = mock_spark

    # 🧪 Exécution
    run_station_job()

    # ✅ Vérifications
    mock_fetch.assert_called_once()
    mock_transform.assert_called_once_with(mock_fetch.return_value)
    mock_schema.assert_called_once()
    mock_spark.createDataFrame.assert_called_once()
    mock_write.assert_called_once_with(mock_df, "hdfs://namenode:9000/velib/raw/stations")
    mock_spark.stop.assert_called_once()


# 🎯 Cas 2 : une exception est levée dans le try
@patch("Jobs.getStationData.write_to_hdfs", side_effect=Exception("Erreur simulée"))
@patch("Jobs.getStationData.SparkSession")
@patch("Jobs.getStationData.get_velib_station_schema")
@patch("Jobs.getStationData.transform_velib_data")
@patch("Jobs.getStationData.fetch_velib_data")
def test_run_station_job_handles_exception(
    mock_fetch,
    mock_transform,
    mock_schema,
    mock_spark_session,
    mock_write
):
    # Config de base pour que tout fonctionne jusqu'à write
    mock_fetch.return_value = [{"stationcode": "123"}]
    mock_transform.return_value = [{"stationcode": "123"}]
    mock_schema.return_value = MagicMock()
    mock_spark = MagicMock()
    mock_spark.createDataFrame.return_value = MagicMock()
    mock_spark_session.builder.appName.return_value.getOrCreate.return_value = mock_spark

    # 🧪 Exécution (l'exception est attrapée dans run_station_job)
    run_station_job()

    # ✅ Vérifie que le SparkSession est quand même arrêté
    mock_spark.stop.assert_called_once()
