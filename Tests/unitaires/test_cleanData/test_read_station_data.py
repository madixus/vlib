from unittest.mock import MagicMock
import pytest
from Jobs.cleanData import read_station_data

def test_read_station_data_success():
    mock_spark = MagicMock()
    mock_df = MagicMock()
    mock_spark.read.parquet.return_value = mock_df

    result = read_station_data(mock_spark)

    mock_spark.read.parquet.assert_called_once_with("hdfs://namenode:9000/velib/raw/stations")
    assert result == mock_df

def test_read_station_data_raises_exception():
    mock_spark = MagicMock()
    mock_spark.read.parquet.side_effect = Exception("Erreur de lecture parquet")

    with pytest.raises(Exception, match="Erreur de lecture parquet"):
        read_station_data(mock_spark)
