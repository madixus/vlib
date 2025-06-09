from Jobs.getDataVelo import get_velib_schema
from pyspark.sql.types import (
    StructType, StringType, IntegerType, BooleanType, DoubleType
)

# Test 1 – Vérifier le nombre de colonnes
def test_schema_field_count():
    schema = get_velib_schema()
    assert len(schema.fields) == 14, "Le schéma doit contenir exactement 14 colonnes"

# Test 2 – Vérifier les noms et types de colonnes
def test_schema_field_names_and_types():
    schema = get_velib_schema()

    expected_fields = [
        ("stationcode", StringType),
        ("name", StringType),
        ("num_bikes_available", IntegerType),
        ("num_docks_available", IntegerType),
        ("mechanical", IntegerType),
        ("ebike", IntegerType),
        ("is_installed", BooleanType),
        ("is_renting", BooleanType),
        ("is_returning", BooleanType),
        ("lon", DoubleType),
        ("lat", DoubleType),
        ("last_reported", StringType),
        ("arrondissement", StringType),
        ("timestamp", StringType),
    ]

    for i, field in enumerate(schema.fields):
        expected_name, expected_type = expected_fields[i]
        assert field.name == expected_name, f"Nom attendu : {expected_name}, obtenu : {field.name}"
        assert isinstance(field.dataType, expected_type), f"Type attendu pour '{field.name}': {expected_type}, obtenu : {type(field.dataType)}"

# Test 3 – Vérifier que le schéma est bien une instance de StructType
from pyspark.sql.types import StructType

def test_schema_type():
    schema = get_velib_schema()
    assert isinstance(schema, StructType), "Le schéma retourné doit être une instance de StructType"