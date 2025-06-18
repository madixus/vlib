from unittest.mock import MagicMock
import pytest
from Jobs.cleanData import read_availability_data

# 🧪 Test 1 – Lecture normale réussie
def test_read_availability_success():
    spark = MagicMock()
    fake_df = MagicMock()
    spark.read.parquet.return_value = fake_df

    df = read_availability_data(spark)

    spark.read.parquet.assert_called_once_with("hdfs://namenode:9000/velib/raw/availability")
    assert df == fake_df, "Le DataFrame retourné doit être celui retourné par parquet()"

# 🧪 Test 2 – Exception levée lors de la lecture (fichier manquant, etc.)
def test_read_availability_file_not_found():
    spark = MagicMock()
    spark.read.parquet.side_effect = Exception("Fichier introuvable")

    with pytest.raises(Exception, match="Fichier introuvable"):
        read_availability_data(spark)

# 🧪 Test 3 – DataFrame vide simulé (via mock)
def test_read_availability_returns_empty_df():
    spark = MagicMock()
    empty_df = MagicMock()
    empty_df.count.return_value = 0
    spark.read.parquet.return_value = empty_df

    df = read_availability_data(spark)

    assert df.count() == 0
