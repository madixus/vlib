from unittest.mock import MagicMock, patch
from Jobs.loadData import read_aggregated_data, write_final_data

# 🧪 Test – Lecture des données agrégées
def test_read_aggregated_data():
    spark = MagicMock()
    fake_df = MagicMock()
    fake_df.count.return_value = 42
    spark.read.parquet.return_value = fake_df

    df = read_aggregated_data(spark)

    spark.read.parquet.assert_called_once_with("hdfs://namenode:9000/velib/aggregation/data")
    fake_df.count.assert_called_once()
    fake_df.show.assert_called_once_with(5)
    assert df == fake_df

# 🧪 Test – Écriture des données finales
def test_write_final_data():
    df = MagicMock()
    write_final_data(df)

    df.write.mode.assert_called_once_with("append")
    df.write.mode().parquet.assert_called_once_with("hdfs://namenode:9000/velib/final/data")
