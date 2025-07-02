from unittest.mock import MagicMock, patch, call
import pytest
from Jobs.cleanData import validate_bike_consistency

class MockColumn:
    def __add__(self, other):
        return self
    def __eq__(self, other):
        return self

def fake_col(name):
    return MockColumn()

def test_validate_bike_consistency_calls_methods_correctly():
    mock_df = MagicMock()
    mock_df.withColumn.return_value = mock_df
    mock_df.filter.return_value = mock_df
    mock_df.drop.return_value = "final_df"

    with patch('Jobs.cleanData.col', side_effect=fake_col) as mock_col:
        result = validate_bike_consistency(mock_df)

        expected_calls = [call("mechanical"), call("ebike"), call("num_bikes_available"), call("bike_check")]
        for expected_call in expected_calls:
            assert expected_call in mock_col.call_args_list

        mock_df.withColumn.assert_called_once_with("bike_check", mock_col("mechanical") + mock_col("ebike"))
        mock_df.filter.assert_called_once_with(mock_col("bike_check") == mock_col("num_bikes_available"))
        mock_df.drop.assert_called_once_with("bike_check")

        assert result == "final_df"

def test_validate_bike_consistency_filter_raises():
    mock_df = MagicMock()
    mock_df.withColumn.return_value = mock_df
    mock_df.filter.side_effect = Exception("Filter error")

    with patch('Jobs.cleanData.col', side_effect=fake_col):
        with pytest.raises(Exception, match="Filter error"):
            validate_bike_consistency(mock_df)

def test_validate_bike_consistency_withColumn_raises():
    mock_df = MagicMock()
    mock_df.withColumn.side_effect = Exception("WithColumn error")

    with patch('Jobs.cleanData.col', side_effect=fake_col):
        with pytest.raises(Exception, match="WithColumn error"):
            validate_bike_consistency(mock_df)
