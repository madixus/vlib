from unittest.mock import MagicMock, patch
import pytest
from Jobs.loadDataPostgresql import create_spark_session


@patch("Jobs.loadDataPostgresql.SparkSession")
def test_create_spark_session_success(mock_spark_session):
    mock_builder = MagicMock()
    mock_session = MagicMock()

    # Chaînage : SparkSession.builder.appName(...).config(...).getOrCreate()
    mock_spark_session.builder = mock_builder
    mock_builder.appName.return_value = mock_builder
    mock_builder.config.return_value = mock_builder
    mock_builder.getOrCreate.return_value = mock_session

    session = create_spark_session()

    mock_builder.appName.assert_called_once_with("Load Velib Data to PostgreSQL")
    mock_builder.config.assert_called_once_with("spark.jars", "/extra-jars/postgresql-42.7.5.jar")
    mock_builder.getOrCreate.assert_called_once()
    assert session == mock_session


@patch("Jobs.loadDataPostgresql.SparkSession")
def test_create_spark_session_failure(mock_spark_session):
    mock_builder = MagicMock()
    mock_spark_session.builder = mock_builder

    mock_builder.appName.return_value = mock_builder
    mock_builder.config.return_value = mock_builder
    mock_builder.getOrCreate.side_effect = Exception("Spark init failed")

    with pytest.raises(Exception, match="Spark init failed"):
        create_spark_session()


@patch("Jobs.loadDataPostgresql.SparkSession")
def test_create_spark_session_config_call(mock_spark_session):
    mock_builder = MagicMock()
    mock_session = MagicMock()
    mock_spark_session.builder = mock_builder

    mock_builder.appName.return_value = mock_builder
    mock_builder.config.return_value = mock_builder
    mock_builder.getOrCreate.return_value = mock_session

    session = create_spark_session()

    assert mock_builder.config.called
