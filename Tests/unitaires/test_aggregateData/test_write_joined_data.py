from unittest.mock import MagicMock
from Jobs.aggregateData import write_joined_data

# 🧪 Test 1 – Vérifie que parquet et csv sont appelés avec les bons arguments
def test_write_joined_data_success(monkeypatch):
    # Mocking du DataFrame et des méthodes chaînées
    write_mock = MagicMock()
    mode_mock = MagicMock()
    parquet_mock = MagicMock()
    option_mock = MagicMock()

    # Simulation du chaînage
    write_mock.mode.return_value = mode_mock
    mode_mock.parquet.return_value = None
    mode_mock.option.return_value = option_mock
    option_mock.csv.return_value = None

    # DataFrame mock avec .write attribué
    joined_df = MagicMock()
    joined_df.write = write_mock

    write_joined_data(joined_df)

    # Vérifications
    write_mock.mode.assert_any_call("overwrite")
    mode_mock.parquet.assert_called_once_with("hdfs://namenode:9000/velib/aggregation/data")
    mode_mock.option.assert_called_once_with("header", True)
    option_mock.csv.assert_called_once_with("hdfs://namenode:9000/velib/aggregation/csvfile")


# 🧪 Test 2 – Erreur lors de l’écriture Parquet
def test_write_joined_data_parquet_error():
    write_mock = MagicMock()
    mode_mock = MagicMock()
    mode_mock.parquet.side_effect = Exception("Erreur parquet")
    write_mock.mode.return_value = mode_mock

    joined_df = MagicMock()
    joined_df.write = write_mock

    try:
        write_joined_data(joined_df)
        assert False, "Une exception aurait dû être levée"
    except Exception as e:
        assert str(e) == "Erreur parquet"

# 🧪 Test 3 – Erreur lors de l’écriture CSV
def test_write_joined_data_csv_error():
    write_mock = MagicMock()
    mode_mock = MagicMock()
    option_mock = MagicMock()

    mode_mock.parquet.return_value = None
    mode_mock.option.return_value = option_mock
    option_mock.csv.side_effect = Exception("Erreur CSV")
    write_mock.mode.return_value = mode_mock

    joined_df = MagicMock()
    joined_df.write = write_mock

    try:
        write_joined_data(joined_df)
        assert False, "Une exception aurait dû être levée"
    except Exception as e:
        assert str(e) == "Erreur CSV"
