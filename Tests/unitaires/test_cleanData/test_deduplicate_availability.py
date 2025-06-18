from unittest.mock import MagicMock
import pytest
from Jobs.cleanData import deduplicate_availability

def test_deduplicate_availability_calls_dropDuplicates_correctly():
    mock_df = MagicMock()
    mock_dedup_df = MagicMock()
    mock_df.dropDuplicates.return_value = mock_dedup_df
    mock_dedup_df.count.return_value = 42  

    result = deduplicate_availability(mock_df)

    mock_df.dropDuplicates.assert_called_once_with(["stationcode", "timestamp"])
    mock_dedup_df.count.assert_called_once()
    assert result == mock_dedup_df
