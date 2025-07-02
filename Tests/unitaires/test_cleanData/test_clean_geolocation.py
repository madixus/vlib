from unittest.mock import MagicMock, patch
import pytest
from Jobs.cleanData import clean_geolocation

class MockColumn:
    def cast(self, _):
        return self
    def __eq__(self, other):
        return False

def fake_col(name):
    return MockColumn()

def test_clean_geolocation_calls_withColumn_correctly():
    mock_df = MagicMock()
    mock_df.withColumn.side_effect = [mock_df, mock_df]  # chaining

    with patch('Jobs.cleanData.col', side_effect=fake_col) as mock_col, \
         patch('Jobs.cleanData.when') as mock_when:
        
        mock_when_instance = MagicMock()
        mock_when.return_value = mock_when_instance
        mock_when_instance.otherwise.return_value = "processed_column"

        result = clean_geolocation(mock_df)

        mock_col.assert_any_call("lat")
        mock_col.assert_any_call("lon")
        assert mock_when.call_count == 2

        expected_calls = [(( "lat", "processed_column"),), (( "lon", "processed_column"),)]
        mock_df.withColumn.assert_has_calls(expected_calls, any_order=False)
        assert result == mock_df

def test_clean_geolocation_handles_backslash_N_values():
    mock_df = MagicMock()
    mock_df.withColumn.side_effect = [mock_df, mock_df]

    with patch('Jobs.cleanData.col', side_effect=fake_col), \
         patch('Jobs.cleanData.when') as mock_when:

        mock_when.return_value.otherwise.return_value = "result"
        clean_geolocation(mock_df)

        assert mock_when.call_count == 2

def test_clean_geolocation_raises_if_withColumn_fails():
    mock_df = MagicMock()
    mock_df.withColumn.side_effect = Exception("Erreur dans withColumn")

    with patch('Jobs.cleanData.col', side_effect=fake_col), \
         patch('Jobs.cleanData.when') as mock_when:

        mock_when.return_value.otherwise.return_value = "result"

        with pytest.raises(Exception, match="Erreur dans withColumn"):
            clean_geolocation(mock_df)
