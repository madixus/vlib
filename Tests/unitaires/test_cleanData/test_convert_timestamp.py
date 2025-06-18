from unittest.mock import MagicMock, patch, call
import pytest
from Jobs.cleanData import convert_timestamp

def test_convert_timestamp_calls_withColumn_correctly():
    mock_df = MagicMock()
    mock_new_df = MagicMock()
    mock_df.withColumn.return_value = mock_new_df

    with patch('Jobs.cleanData.col') as mock_col, patch('Jobs.cleanData.to_timestamp') as mock_to_ts:
        mock_col.return_value = "mocked_col"
        mock_to_ts.return_value = "timestamp_column"

        result = convert_timestamp(mock_df)

        mock_col.assert_called_once_with("last_reported")
        mock_to_ts.assert_called_once_with("mocked_col", "yyyy-MM-dd'T'HH:mm:ssXXX")
        mock_df.withColumn.assert_called_once_with("event_ts", "timestamp_column")

        # On s'assure que select() et show() ont été appelés aussi
        mock_new_df.select.assert_called_once_with("last_reported", "event_ts")
        mock_new_df.select.return_value.show.assert_called_once_with(5, truncate=False)

        assert result == mock_new_df

def test_convert_timestamp_raises_if_withColumn_fails():
    mock_df = MagicMock()
    mock_df.withColumn.side_effect = Exception("withColumn error")

    with patch('Jobs.cleanData.col') as mock_col, patch('Jobs.cleanData.to_timestamp') as mock_to_ts:
        mock_col.return_value = "some_col"
        mock_to_ts.return_value = "some_timestamp"

        with pytest.raises(Exception, match="withColumn error"):
            convert_timestamp(mock_df)
