from unittest.mock import MagicMock
import pytest
from Jobs.getDataVelo import write_to_hdfs  

# Cas standard : vérifier que df.write.mode("overwrite").parquet(path) est appelé par défaut.
def test_write_to_hdfs_calls_parquet_default_mode():
    mock_df = MagicMock()
    mock_write = mock_df.write
    mock_mode = mock_write.mode.return_value
    mock_mode.parquet.return_value = None

    write_to_hdfs(mock_df, "hdfs://test/path")

    mock_write.mode.assert_called_once_with("overwrite")  # <-- ici le changement
    mock_mode.parquet.assert_called_once_with("hdfs://test/path")

# Cas alternatif : le mode est passé explicitement en paramètre (e.g. "append").
def test_write_to_hdfs_calls_parquet_custom_mode():
    mock_df = MagicMock()
    mock_write = mock_df.write
    mock_mode = mock_write.mode.return_value
    mock_mode.parquet.return_value = None

    write_to_hdfs(mock_df, "hdfs://custom/path", mode="append")

    mock_write.mode.assert_called_once_with("append")  
    mock_mode.parquet.assert_called_once_with("hdfs://custom/path")

# Cas d'erreur : simuler une exception lors de l'appel à parquet().
def test_write_to_hdfs_raises_exception_on_write_error():
    mock_df = MagicMock()
    mock_write = mock_df.write
    mock_mode = mock_write.mode.return_value
    mock_mode.parquet.side_effect = Exception("Erreur simulée lors de l'écriture")

    with pytest.raises(Exception, match="Erreur simulée lors de l'écriture"):
        write_to_hdfs(mock_df, "hdfs://fail/path")
