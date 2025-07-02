import pytest
from unittest.mock import MagicMock, patch
from Jobs.loadDataPostgresql import read_source_data


def test_read_source_data_success():
    # Mock du DataFrame retourné par spark.read.parquet
    mock_df = MagicMock()
    mock_df.count.return_value = 100

    # Mock du spark.read.parquet
    mock_read = MagicMock()
    mock_read.parquet.return_value = mock_df

    spark = MagicMock()
    spark.read = mock_read

    result = read_source_data(spark, "hdfs://path/to/data")

    assert result == mock_df
    mock_read.parquet.assert_called_once_with("hdfs://path/to/data")
    mock_df.count.assert_called_once()


def test_read_source_data_empty():
    mock_df = MagicMock()
    mock_df.count.return_value = 0

    mock_read = MagicMock()
    mock_read.parquet.return_value = mock_df

    spark = MagicMock()
    spark.read = mock_read

    result = read_source_data(spark, "hdfs://path/to/empty-data")

    assert result == mock_df
    mock_df.count.assert_called_once()


def test_read_source_data_error():
    # Simuler une erreur de lecture Parquet
    mock_read = MagicMock()
    mock_read.parquet.side_effect = Exception("Parquet read failed")

    spark = MagicMock()
    spark.read = mock_read

    with pytest.raises(Exception, match="Parquet read failed"):
        read_source_data(spark, "hdfs://path/to/invalid")
