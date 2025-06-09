import pytest
from Jobs.getStationData import transform_velib_data

# test : cas standard
def test_transform_velib_data_basic():
    input_data = [
        {
            "stationcode": "123",
            "name": "Station A",
            "capacity": 20,
            "coordonnees_geo": {"lon": 2.35, "lat": 48.85},
            "station_opening_hours": "24/7"
        }
    ]

    result = transform_velib_data(input_data)

    assert len(result) == 1
    transformed = result[0]

    assert transformed["stationcode"] == "123"
    assert transformed["name"] == "Station A"
    assert transformed["capacity"] == 20
    assert transformed["lon"] == 2.35
    assert transformed["lat"] == 48.85
    assert transformed["station_opening_hours"] == "24/7"
    assert "timestamp" in transformed

# cas: data missing
def test_transform_velib_data_missing_coordinates():
    input_data = [
        {
            "stationcode": "999",
            "name": "Station X",
            "capacity": 10,
            "station_opening_hours": "Closed"
            # coordonnees_geo is missing
        }
    ]

    result = transform_velib_data(input_data)
    transformed = result[0]

    assert transformed["lon"] is None
    assert transformed["lat"] is None

# cas multiple
def test_transform_velib_data_multiple_entries():
    input_data = [
        {
            "stationcode": "1",
            "name": "Station One",
            "capacity": 15,
            "coordonnees_geo": {"lon": 2.3, "lat": 48.8},
            "station_opening_hours": "7am-7pm"
        },
        {
            "stationcode": "2",
            "name": "Station Two",
            "capacity": 25,
            "coordonnees_geo": {"lon": 2.4, "lat": 48.9},
            "station_opening_hours": "24h"
        }
    ]

    result = transform_velib_data(input_data)

    assert len(result) == 2
    # Assure-toi que le timestamp est identique pour les deux (car généré en une fois)
    assert result[0]["timestamp"] == result[1]["timestamp"]
