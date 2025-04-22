from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType, LongType, BooleanType, DoubleType
import requests
from datetime import datetime

# ---------- CONFIGURATION ----------
BASE_URL = "https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/velib-emplacement-des-stations/records"
LIMIT = 100

# ---------- VELIB FETCH ----------
def fetch_velib_data():
    all_records = []
    offset = 0

    while True:
        url = f"{BASE_URL}?limit={LIMIT}&offset={offset}"
        print(f" Récupération en cours : {url}")
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        results = data.get("results", [])

        all_records.extend(results)

        if len(results) < LIMIT:
            break  # fin des données
        offset += LIMIT

    print(f" Total location and caracteristics stations récupérées avec succées: {len(all_records)}")
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
            "capacity": f.get("capacity"),
            "lon": f.get("coordonnees_geo", {}).get("lon"),
            "lat": f.get("coordonnees_geo", {}).get("lat"),
            "station_opening_hours": f.get("station_opening_hours"),
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
        .add("capacity", IntegerType()) \
        .add("lon", DoubleType()) \
        .add("lat", DoubleType()) \
        .add("station_opening_hours", StringType()) \
        .add("timestamp", StringType())

    # DataFrame + écriture HDFS
    velib_df = spark.createDataFrame(velib_rows, schema=velib_schema)
    velib_df.write.mode("overwrite").parquet("hdfs://namenode:9000/velib/raw/stations")
    print("🚴 Données Vélib station écrites sur HDFS")

    spark.stop()

if __name__ == "__main__":
    main()
