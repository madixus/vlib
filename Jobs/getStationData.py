from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType, DoubleType
import requests
from datetime import datetime

# ---------- CONFIG ----------
STATIONS_URL = "https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/velib-emplacement-des-stations/records"
LIMIT = 100

# ---------- FETCH ----------
def fetch_station_data():
    all_stations = []
    offset = 0

    while True:
        url = f"{STATIONS_URL}?limit={LIMIT}&offset={offset}"
        print(f"Récupération en cours : {url}")
        response = requests.get(url)
        response.raise_for_status()
        data = response.json().get("results", [])
        all_stations.extend(data)

        if len(data) < LIMIT:
            break
        offset += LIMIT

    print(f"Total stations récupérées : {len(all_stations)}")
    return all_stations

# ---------- TRANSFORM ----------
def transform_station_data(records, timestamp=None):
    if timestamp is None:
        timestamp = datetime.utcnow().isoformat()

    transformed = []

    for r in records:
        coords = r.get("coordonnees_geo", {})
        transformed.append({
            "stationcode": r.get("stationcode"),
            "name": r.get("name"),
            "capacity": r.get("capacity"),
            "lon": coords.get("lon"),
            "lat": coords.get("lat"),
            "station_opening_hours": r.get("station_opening_hours"),
            "timestamp": timestamp
        })

    return transformed

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
    spark = SparkSession.builder.appName("VelibStationIngestion").getOrCreate()
    spark.conf.set("spark.hadoop.dfs.replication", "1")

    try:
        records = fetch_station_data()
        rows = transform_station_data(records)
        schema = get_velib_station_schema()
        df = spark.createDataFrame(rows, schema=schema)
        write_to_hdfs(df, "hdfs://namenode:9000/velib/raw/stations")
        print("🚴 Données Vélib station écrites sur HDFS")
    except Exception as e:
        print(f"❌ Erreur lors de l'écriture dans HDFS : {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    run_station_job()
