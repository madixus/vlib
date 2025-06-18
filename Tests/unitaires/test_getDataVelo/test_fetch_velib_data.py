import requests # type: ignore
from unittest.mock import patch
from Jobs.getDataVelo import fetch_velib_data

@patch("Jobs.getDataVelo.requests.get")
def test_fetch_velib_data(mock_get):
    # Fake API response
    mock_get.return_value.status_code = 200
    mock_get.return_value.json.return_value = {
        "results": [{"stationcode": "test"}]
    }

    data = fetch_velib_data()
    assert isinstance(data, list)
    assert data[0]["stationcode"] == "test"

@patch("Jobs.getDataVelo.requests.get")
def test_fetch_velib_data_http_error(mock_get):
    mock_get.return_value.raise_for_status.side_effect = requests.HTTPError("API Error 500")

    try:
        fetch_velib_data()
        assert False, "Une exception aurait dû être levée"
    except requests.HTTPError as e:
        assert str(e) == "API Error 500"

@patch("Jobs.getDataVelo.requests.get")
def test_fetch_velib_data_empty_results(mock_get):
    mock_get.return_value.status_code = 200
    mock_get.return_value.json.return_value = {}

    data = fetch_velib_data()
    assert isinstance(data, list)
    assert len(data) == 0



@patch("Jobs.getDataVelo.requests.get")
def test_fetch_velib_data_pagination(mock_get):
    # Simule 2 pages : une avec LIMIT résultats, puis une dernière avec moins
    mock_get.side_effect = [
        MockResponse([{"stationcode": "A"}] * 100),
        MockResponse([{"stationcode": "B"}] * 10),
    ]

    data = fetch_velib_data()
    assert len(data) == 110
    assert data[0]["stationcode"] == "A"
    assert data[-1]["stationcode"] == "B"

# Helper pour simuler les réponses paginées
class MockResponse:
    def __init__(self, results):
        self.status_code = 200
        self._results = results

    def raise_for_status(self):
        pass

    def json(self):
        return {"results": self._results}
