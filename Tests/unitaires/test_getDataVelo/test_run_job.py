import pytest
from unittest.mock import patch, MagicMock
from Jobs.getDataVelo import run_job


@patch("Jobs.getDataVelo.SparkSession")
@patch("Jobs.getDataVelo.fetch_velib_data")
@patch("Jobs.getDataVelo.transform_velib_data")
@patch("Jobs.getDataVelo.get_velib_schema")
@patch("Jobs.getDataVelo.write_to_hdfs")

#     Test standard : run_job() s'exécute avec succès.
def test_run_job_success(mock_write, mock_schema, mock_transform, mock_fetch, mock_spark):
    # 🔧 Données simulées
    mock_fetch.return_value = [{"stationcode": "123"}]
    mock_transform.return_value = [{"stationcode": "123"}]
    mock_schema.return_value = "fake_schema"

    # 🎭 Spark mock
    mock_df = MagicMock()
    mock_session = MagicMock()
    mock_session.createDataFrame.return_value = mock_df
    mock_spark.builder.appName.return_value.getOrCreate.return_value = mock_session

    run_job()

    # ✅ Vérifications
    mock_fetch.assert_called_once()
    mock_transform.assert_called_once()
    mock_schema.assert_called_once()
    mock_session.createDataFrame.assert_called_once_with(
        mock_transform.return_value,
        schema=mock_schema.return_value
    )
    mock_write.assert_called_once_with(
        mock_df,
        "hdfs://namenode:9000/velib/raw/availability_v2"
    )
    mock_session.stop.assert_called_once()


@patch("Jobs.getDataVelo.SparkSession")
@patch("Jobs.getDataVelo.fetch_velib_data", side_effect=Exception("Erreur simulée"))

#     Cas d'erreur : une exception se produit pendant le job.
def test_run_job_failure(mock_fetch, mock_spark):
    mock_session = MagicMock()
    mock_spark.builder.appName.return_value.getOrCreate.return_value = mock_session

    run_job()

    # ✅ Le SparkSession est arrêté même en cas d'erreur
    mock_session.stop.assert_called_once()
