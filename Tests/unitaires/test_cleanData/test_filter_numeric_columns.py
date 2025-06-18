import pytest
from unittest.mock import MagicMock, patch
from pyspark.sql import DataFrame
from Jobs.cleanData import filter_numeric_columns

class MockColumn:
    def __ge__(self, other):
        return self
    def __and__(self, other):
        return self

def fake_col(name):
    return MockColumn()

def test_filter_numeric_columns_happy_path():
    mock_df = MagicMock(spec=DataFrame)
    with patch('Jobs.cleanData.col', side_effect=fake_col):
        result = filter_numeric_columns(mock_df)
        mock_df.filter.assert_called_once()  # On vérifie que filter a été appelé
        assert result == mock_df.filter.return_value

def test_filter_numeric_columns_with_filter_exception():
    mock_df = MagicMock(spec=DataFrame)
    mock_df.filter.side_effect = Exception("Filter failure")
    with patch('Jobs.cleanData.col', side_effect=fake_col):
        with pytest.raises(Exception, match="Filter failure"):
            filter_numeric_columns(mock_df)

def test_filter_numeric_columns_calls_filter_once():
    mock_df = MagicMock(spec=DataFrame)
    with patch('Jobs.cleanData.col', side_effect=fake_col):
        filter_numeric_columns(mock_df)
        assert mock_df.filter.call_count == 1
