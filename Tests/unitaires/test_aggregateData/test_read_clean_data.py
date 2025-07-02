from unittest.mock import MagicMock
import pytest
from Jobs.aggregateData import read_clean_data

# 🧪 Test 1 – Lecture réussie des deux DataFrames
def test_read_clean_data_success():
    spark = MagicMock()
    mock_avail_df = MagicMock()
    mock_station_df = MagicMock()

    # Simule les appels à parquet()
    spark.read.parquet.side_effect = [mock_avail_df, mock_station_df]

    avail_df, station_df = read_clean_data(spark)

    expected_calls = [
        "hdfs://namenode:9000/velib/clean/availability",
        "hdfs://namenode:9000/velib/clean/stations"
    ]

    actual_calls = [call.args[0] for call in spark.read.parquet.call_args_list]
    assert actual_calls == expected_calls

    assert avail_df == mock_avail_df
    assert station_df == mock_station_df


# 🧪 Test 2 – Erreur lors de la lecture de availability.parquet
def test_read_clean_data_availability_missing():
    spark = MagicMock()
    spark.read.parquet.side_effect = Exception("availability introuvable")

    with pytest.raises(Exception, match="availability introuvable"):
        read_clean_data(spark)


# 🧪 Test 3 – Erreur lors de la lecture de stations.parquet
def test_read_clean_data_station_missing():
    spark = MagicMock()
    mock_avail_df = MagicMock()
    # Simule : le 1er appel réussit, le 2e échoue
    spark.read.parquet.side_effect = [mock_avail_df, Exception("stations introuvable")]

    with pytest.raises(Exception, match="stations introuvable"):
        read_clean_data(spark)
