from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType, DoubleType
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
        print(f"Récupération en cours : {url}")
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        results = data.get("results", [])

        all_records.extend(results)
        if len(results) < LIMIT:
            break
        offset += LIMIT

    print(f"Total stations récupérées : {len(all_records)}")
    return all_records

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

# ---------- SCHEMA ----------
def get_velib_station_schema():
    return StructType() \
        .add("stationcode", StringType()) \
        .add("name", StringType()) \
        .add("capacity", IntegerType()) \
        .add("lon", DoubleType()) \
        .add("lat", DoubleType()) \
        .add("station_opening_hours", StringType()) \
        .add("timestamp", StringType())

# ---------- WRITE ----------
def write_to_hdfs(df, path, mode="overwrite"):
    df.write.mode(mode).parquet(path)

# ---------- MAIN ----------
def run_station_job():
    spark = SparkSession.builder.appName("VelibDataIngestionV2").getOrCreate()
    spark.conf.set("spark.hadoop.dfs.replication", "1")

    try:
        records = fetch_velib_data()
        rows = transform_velib_data(records)
        schema = get_velib_station_schema()
        df = spark.createDataFrame(rows, schema=schema)
        write_to_hdfs(df, "hdfs://namenode:9000/velib/raw/stations")
        print("🚴 Données Vélib station écrites sur HDFS")
    except Exception as e:
        print(f"Erreur lors de l'écriture dans HDFS : {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    run_station_job()
