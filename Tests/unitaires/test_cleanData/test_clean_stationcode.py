from unittest.mock import MagicMock, patch
import pytest
from pyspark.sql.functions import trim, col
from Jobs.cleanData import clean_stationcode

def test_clean_stationcode_calls_with_column():
    mock_df = MagicMock()
    mock_with_column = mock_df.withColumn
    mock_result_df = MagicMock()
    mock_with_column.return_value = mock_result_df
    
    # Patch col et trim pour éviter appel réel
    with patch('Jobs.cleanData.col') as mock_col, patch('Jobs.cleanData.trim') as mock_trim:
        mock_col.return_value.cast.return_value = "casted_col"
        mock_trim.return_value = "trimmed_col"

        result = clean_stationcode(mock_df)

        mock_col.assert_called_once_with("stationcode")
        mock_col.return_value.cast.assert_called_once_with("string")
        mock_trim.assert_called_once_with("casted_col")
        mock_with_column.assert_called_once_with("stationcode", "trimmed_col")
        assert result == mock_result_df

def test_clean_stationcode_raises_if_withColumn_fails():
    mock_df = MagicMock()
    mock_df.withColumn.side_effect = Exception("Column not found")

    with patch('Jobs.cleanData.col') as mock_col, patch('Jobs.cleanData.trim') as mock_trim:
        # Mocker col() et trim() pour éviter appel réel
        mock_col.return_value.cast.return_value = "casted_col"
        mock_trim.return_value = "trimmed_col"

        with pytest.raises(Exception, match="Column not found"):
            clean_stationcode(mock_df)
