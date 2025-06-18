from unittest.mock import MagicMock, patch
import pytest
from pyspark.sql import DataFrame
from Jobs.cleanData import filter_valid_geolocation

# === Mocks partagés ===

class MockColumn:
    def isNotNull(self):
        return self
    def __and__(self, other):
        return "filter_condition"

def fake_col(name):
    return MockColumn()

# === Tests ===

def test_filter_valid_geolocation_calls_filter_correctly():
    mock_filtered_df = MagicMock(spec=DataFrame)
    mock_filtered_df.count.return_value = 42  # valeur arbitraire

    mock_df = MagicMock(spec=DataFrame)
    mock_df.filter.return_value = mock_filtered_df

    with patch('Jobs.cleanData.col', side_effect=fake_col):
        result = filter_valid_geolocation(mock_df)
        mock_df.filter.assert_called_once_with("filter_condition")
        mock_filtered_df.count.assert_called_once()
        assert result == mock_filtered_df


def test_filter_valid_geolocation_filter_raises():
    mock_df = MagicMock(spec=DataFrame)
    mock_df.filter.side_effect = Exception("Erreur dans filter")

    with patch('Jobs.cleanData.col', side_effect=fake_col):
        with pytest.raises(Exception, match="Erreur dans filter"):
            filter_valid_geolocation(mock_df)
