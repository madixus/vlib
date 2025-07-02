from unittest.mock import MagicMock
import pytest
from Jobs.getStationData import write_to_hdfs  

# 🧪 Cas 1 : Mode par défaut ("overwrite") utilisé si aucun mode n'est spécifié.
def test_write_to_hdfs_calls_parquet_with_default_mode():
    mock_df = MagicMock()
    
    mock_write = mock_df.write
    mock_mode = mock_write.mode.return_value
    mock_mode.parquet.return_value = None

    write_to_hdfs(mock_df, "hdfs://test/path")

    mock_write.mode.assert_called_once_with("overwrite")

    mock_mode.parquet.assert_called_once_with("hdfs://test/path")


# 🧪 Cas 2 : Mode personnalisé (ex: "append") passé explicitement
def test_write_to_hdfs_calls_parquet_with_custom_mode():
    mock_df = MagicMock()
    mock_write = mock_df.write
    mock_mode = mock_write.mode.return_value
    mock_mode.parquet.return_value = None

    write_to_hdfs(mock_df, "hdfs://another/path", mode="append")

    mock_write.mode.assert_called_once_with("append")

    mock_mode.parquet.assert_called_once_with("hdfs://another/path")


# 🧪 Cas 3 : Simulation d'une erreur pendant l'écriture
def test_write_to_hdfs_raises_exception_on_failure():
    mock_df = MagicMock()
    mock_write = mock_df.write
    mock_mode = mock_write.mode.return_value

    mock_mode.parquet.side_effect = Exception("Erreur d'écriture simulée")

    with pytest.raises(Exception, match="Erreur d'écriture simulée"):
        write_to_hdfs(mock_df, "hdfs://fail/path", mode="overwrite")
