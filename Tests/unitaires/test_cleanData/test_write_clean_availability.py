from unittest.mock import MagicMock
import pytest
from Jobs.cleanData import write_clean_availability

def test_write_clean_availability_calls_write_mode_parquet_correctly():
    mock_df = MagicMock()
    mock_write = MagicMock()
    mock_mode = MagicMock()

    mock_df.write = mock_write
    mock_write.mode.return_value = mock_mode
    mock_mode.parquet.return_value = None  # méthode parquet ne retourne rien

    write_clean_availability(mock_df)

    mock_write.mode.assert_called_once_with("overwrite")
    mock_mode.parquet.assert_called_once_with("hdfs://namenode:9000/velib/clean/availability")

def test_write_clean_availability_raises_if_parquet_fails():
    mock_df = MagicMock()
    mock_write = MagicMock()
    mock_mode = MagicMock()

    mock_df.write = mock_write
    mock_write.mode.return_value = mock_mode
    mock_mode.parquet.side_effect = Exception("Erreur écriture parquet")

    with pytest.raises(Exception, match="Erreur écriture parquet"):
        write_clean_availability(mock_df)
