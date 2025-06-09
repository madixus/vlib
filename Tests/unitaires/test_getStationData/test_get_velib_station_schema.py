from pyspark.sql.types import StructType, StringType, IntegerType, DoubleType
from Jobs.getStationData import get_velib_station_schema

# Vérifie que le schéma est bien un StructType
def test_get_velib_station_schema_fields():
    schema = get_velib_station_schema()
    assert isinstance(schema, StructType)

    expected_fields = [
        "stationcode",
        "name",
        "capacity",
        "lon",
        "lat",
        "station_opening_hours",
        "timestamp"
    ]
    actual_fields = [field.name for field in schema.fields]

    assert actual_fields == expected_fields

# Vérifie que les types de chaque champ dans le schéma sont corrects.
def test_get_velib_station_schema_field_types():
    schema = get_velib_station_schema()

    expected_types = [
        StringType,    # stationcode
        StringType,    # name
        IntegerType,   # capacity
        DoubleType,    # lon
        DoubleType,    # lat
        StringType,    # station_opening_hours
        StringType     # timestamp
    ]
    actual_types = [type(field.dataType) for field in schema.fields]

    for actual, expected in zip(actual_types, expected_types):
        assert actual == expected
