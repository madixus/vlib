from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType, LongType, BooleanType, DoubleType
import requests
from datetime import datetime

# ---------- CONFIGURATION ----------
BASE_URL = "https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/velib-disponibilite-en-temps-reel/records"
LIMIT = 100

# ---------- VELIB FETCH ----------
def fetch_velib_data():
    all_records = []
    offset = 0

    while True:
        url = f"{BASE_URL}?limit={LIMIT}&offset={offset}"
        print(f"Récupération en cours  : {url}")
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        results = data.get("results", [])

        all_records.extend(results)

        if len(results) < LIMIT:
            break  # fin des données
        offset += LIMIT

    print(f"Total stations récupérées avec succées: {len(all_records)}")
    return all_records

def to_bool(value):
    return str(value).strip().lower() == "oui"

# ---------- TRANSFORMATION ----------
def transform_velib_data(records):
    now = datetime.utcnow().isoformat()
    rows = []
    for f in records:
        rows.append({
            "stationcode": f.get("stationcode"),
            "name": f.get("name"),
            "num_bikes_available": f.get("numbikesavailable"),
            "num_docks_available": f.get("numdocksavailable"),
            "mechanical": f.get("mechanical"),
            "ebike": f.get("ebike"),
            "is_installed": to_bool(f.get("is_installed")),
            "is_renting": to_bool(f.get("is_renting")),
            "is_returning": to_bool(f.get("is_returning")),
            "lon": f.get("coordonnees_geo", {}).get("lon"),
            "lat": f.get("coordonnees_geo", {}).get("lat"),
            "last_reported": f.get("duedate"),
            "arrondissement": f.get("nom_arrondissement_communes"),
            "timestamp": now
        })
    return rows

# ---------- MAIN ----------
def main():
    spark = SparkSession.builder.appName("VelibDataIngestionV2").getOrCreate()
    spark.conf.set("spark.hadoop.dfs.replication", "1")

    # Récupération + transformation
    velib_data = fetch_velib_data()
    velib_rows = transform_velib_data(velib_data)

    # Schéma
    velib_schema = StructType() \
        .add("stationcode", StringType()) \
        .add("name", StringType()) \
        .add("num_bikes_available", IntegerType()) \
        .add("num_docks_available", IntegerType()) \
        .add("mechanical", IntegerType()) \
        .add("ebike", IntegerType()) \
        .add("is_installed", BooleanType()) \
        .add("is_renting", BooleanType()) \
        .add("is_returning", BooleanType()) \
        .add("lon", DoubleType()) \
        .add("lat", DoubleType()) \
        .add("last_reported", StringType()) \
        .add("arrondissement", StringType()) \
        .add("timestamp", StringType())

    # DataFrame + écriture HDFS
    velib_df = spark.createDataFrame(velib_rows, schema=velib_schema)
    velib_df.write.mode("append").parquet("hdfs://namenode:9000/velib/raw/availability_v2")
    print("🚴 Données Vélib écrites sur HDFS")

    spark.stop()

if __name__ == "__main__":
    main()
