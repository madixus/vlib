from Jobs.getDataVelo import transform_velib_data

# Cas standard
def test_transform_standard_case():
    input_data = [
        {
            "stationcode": "1001",
            "name": "Station République",
            "numbikesavailable": 5,
            "numdocksavailable": 10,
            "mechanical": 3,
            "ebike": 2,
            "is_installed": "OUI",
            "is_renting": "NON",
            "is_returning": "OUI",
            "coordonnees_geo": {"lon": 2.3700, "lat": 48.867},
            "duedate": "2025-05-27T12:00:00Z",
            "nom_arrondissement_communes": "Paris 10e"
        }
    ]

    result = transform_velib_data(input_data, timestamp="2025-05-28T12:00:00Z")

    assert len(result) == 1
    row = result[0]
    assert row["stationcode"] == "1001"
    assert row["is_installed"] is True
    assert row["is_renting"] is False
    assert row["is_returning"] is True
    assert row["lat"] == 48.867
    assert row["timestamp"] == "2025-05-28T12:00:00Z"

# Données incomplètes – champs manquants
def test_transform_missing_fields():
    input_data = [
        {
            "stationcode": "1002",
            # "name" is missing
            # "coordonnees_geo" is missing
        }
    ]

    result = transform_velib_data(input_data)
    row = result[0]
    assert row["name"] is None
    assert row["lon"] is None
    assert row["lat"] is None
    assert isinstance(row["timestamp"], str)

# Champs booléens mal formés
def test_transform_invalid_booleans():
    input_data = [
        {
            "stationcode": "1003",
            "is_installed": "yes",
            "is_renting": "no",
            "is_returning": None
        }
    ]

    result = transform_velib_data(input_data)
    row = result[0]
    assert row["is_installed"] is False
    assert row["is_renting"] is False
    assert row["is_returning"] is False

#Liste vide
def test_transform_empty_input():
    result = transform_velib_data([])
    assert result == []