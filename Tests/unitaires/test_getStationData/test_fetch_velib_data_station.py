import pytest
from unittest.mock import patch, MagicMock
from Jobs.getStationData import fetch_velib_data

# ----------- Cas standard : pagination normale -----------
@patch("Jobs.getStationData.requests.get")
def test_fetch_velib_data_success(mock_get):
    # Simule 2 pages de 100 et 20 éléments
    first_response = MagicMock()
    first_response.json.return_value = {"results": [{"id": i} for i in range(100)]}
    first_response.status_code = 200
    second_response = MagicMock()
    second_response.json.return_value = {"results": [{"id": i} for i in range(100, 120)]}
    second_response.status_code = 200

    mock_get.side_effect = [first_response, second_response]

    results = fetch_velib_data()
    assert len(results) == 120
    assert results[0]["id"] == 0
    assert results[-1]["id"] == 119
    assert mock_get.call_count == 2

# ----------- Cas limite : pas de données -----------
@patch("Jobs.getStationData.requests.get")
def test_fetch_velib_data_empty(mock_get):
    response = MagicMock()
    response.json.return_value = {"results": []}
    response.status_code = 200
    mock_get.return_value = response

    results = fetch_velib_data()
    assert results == []
    assert mock_get.call_count == 1

# ----------- Cas erreur HTTP -----------
@patch("Jobs.getStationData.requests.get")
def test_fetch_velib_data_http_error(mock_get):
    mock_response = MagicMock()
    mock_response.raise_for_status.side_effect = Exception("HTTP error")
    mock_get.return_value = mock_response

    with pytest.raises(Exception, match="HTTP error"):
        fetch_velib_data()

# ----------- Cas JSON invalide -----------
@patch("Jobs.getStationData.requests.get")
def test_fetch_velib_data_invalid_json(mock_get):
    mock_response = MagicMock()
    mock_response.json.side_effect = ValueError("Invalid JSON")
    mock_get.return_value = mock_response

    with pytest.raises(ValueError, match="Invalid JSON"):
        fetch_velib_data()
