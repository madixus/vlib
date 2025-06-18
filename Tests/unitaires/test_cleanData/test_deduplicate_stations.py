from unittest.mock import MagicMock
import pytest
from pyspark.sql import DataFrame
from Jobs.cleanData import deduplicate_stations

# === Mocks partagés ===

mock_df = MagicMock(spec=DataFrame)

# === Tests ===

def test_deduplicate_stations_calls_dropDuplicates():
    mock_dedup_df = MagicMock(spec=DataFrame)
    mock_dedup_df.count.return_value = 42 

    mock_df.dropDuplicates.return_value = mock_dedup_df

    result = deduplicate_stations(mock_df)

    mock_df.dropDuplicates.assert_called_once_with(["stationcode"])
    mock_dedup_df.count.assert_called_once()
    assert result == mock_dedup_df


def test_deduplicate_stations_raises_if_drop_fails():
    mock_df.dropDuplicates.side_effect = Exception("Erreur dans dropDuplicates")

    with pytest.raises(Exception, match="Erreur dans dropDuplicates"):
        deduplicate_stations(mock_df)
