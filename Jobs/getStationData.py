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
        response = requests.get(url)
        response.raise_for_status()
        data = response.json().get("results", [])
        all_stations.extend(data)

        if len(data) < LIMIT:
            break
        offset += LIMIT

    return all_stations

# ---------- TRANSFORM ----------
def transform_station_data(records):
    now = datetime.utcnow().isoformat()
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
            "timestamp": now
        })

    return transformed

# ---------- MAIN ----------
def main():
    spark = SparkSession.builder.appName("VelibStationIngestion").getOrCreate()

    data = fetch_station_data()
    rows = transform_station_data(data)

    schema = StructType() \
        .add("stationcode", StringType()) \
        .add("name", StringType()) \
        .add("capacity", IntegerType()) \
        .add("lon", DoubleType()) \
        .add("lat", DoubleType()) \
        .add("station_opening_hours", StringType()) \
        .add("timestamp", StringType())

    df = spark.createDataFrame(rows, schema=schema)
    print(f"[getStationData] Nombre de stations récupérées : {df.count()}")
    df.show(5)
    df.write.mode("overwrite").parquet("hdfs://namenode:9000/velib/raw/stations")

    print(" Stations enregistrées dans HDFS.")
    spark.stop()

if __name__ == "__main__":
    main()
