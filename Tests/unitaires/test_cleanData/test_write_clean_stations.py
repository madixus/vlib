from unittest.mock import MagicMock
import pytest
from Jobs.cleanData import write_clean_stations

# === Mocks partagés ===

mock_df = MagicMock()
mock_writer = MagicMock()
mock_mode_writer = MagicMock()

# Configuration du chaînage : df.write.mode(...).parquet(...)
mock_df.write = mock_writer
mock_writer.mode.return_value = mock_mode_writer

# === Tests ===

def test_write_clean_stations_calls_write_correctly():
    write_clean_stations(mock_df)

    mock_writer.mode.assert_called_once_with("overwrite")
    mock_mode_writer.parquet.assert_called_once_with("hdfs://namenode:9000/velib/clean/stations")


def test_write_clean_stations_raises_on_failure():
    mock_writer.mode.side_effect = Exception("Erreur d'écriture")

    with pytest.raises(Exception, match="Erreur d'écriture"):
        write_clean_stations(mock_df)
