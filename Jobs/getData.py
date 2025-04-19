from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType, LongType, BooleanType, FloatType
import requests
from datetime import datetime

# ---------- CONFIGURATION ----------
LAT = 48.8566
LON = 2.3522
METEO_API_URL = f"https://api.open-meteo.com/v1/forecast?latitude={LAT}&longitude={LON}&hourly=temperature_2m,relative_humidity_2m,wind_speed_10m,precipitation&timezone=Europe%2FParis"
VELIB_API_URL = "https://opendata.paris.fr/api/records/1.0/search/?dataset=velib-disponibilite-en-temps-reel&rows=1000"

# ---------- MÉTÉO ----------
def fetch_weather_data():
    response = requests.get(METEO_API_URL)
    response.raise_for_status()
    return response.json()

def transform_weather_data(json_data):
    hourly = json_data.get("hourly", {})
    rows = []
    now = datetime.utcnow().isoformat()
    for i in range(len(hourly["time"])):
        rows.append({
            "time": hourly["time"][i],
            "temperature": float(hourly["temperature_2m"][i]),
            "humidity": float(hourly["relative_humidity_2m"][i]),
            "wind_speed": float(hourly["wind_speed_10m"][i]),
            "precipitation": float(hourly["precipitation"][i]),
            "ingestion_ts": now
        })
    return rows

# ---------- VELIB ----------
def fetch_velib_data():
    response = requests.get(VELIB_API_URL)
    response.raise_for_status()
    return response.json()["records"]

def to_bool(value):
    return str(value).strip().lower() == "oui"

def transform_velib_data(records):
    now = datetime.utcnow().isoformat()
    rows = []
    for rec in records:
        f = rec.get("fields", {})
        rows.append({
            "stationcode": f.get("stationcode"),
            "num_bikes_available": f.get("numbikesavailable"),
            "num_docks_available": f.get("numdocksavailable"),
            "is_installed": to_bool(f.get("is_installed")),
            "is_renting": to_bool(f.get("is_renting")),
            "is_returning": to_bool(f.get("is_returning")),
            "last_reported": f.get("last_reported"),
            "timestamp": now
        })
    return rows

# ---------- MAIN ----------
def main():
    spark = SparkSession.builder.appName("DataIngestion").getOrCreate()
    spark.conf.set("spark.hadoop.dfs.replication", "1")

    # Météo
    meteo_data = fetch_weather_data()
    meteo_rows = transform_weather_data(meteo_data)

    meteo_schema = StructType() \
        .add("time", StringType()) \
        .add("temperature", FloatType()) \
        .add("humidity", FloatType()) \
        .add("wind_speed", FloatType()) \
        .add("precipitation", FloatType()) \
        .add("ingestion_ts", StringType())

    meteo_df = spark.createDataFrame(meteo_rows, schema=meteo_schema)
    meteo_df.write.mode("append").parquet("hdfs://namenode:9000/weather/paris")
    print("✅ Données météo enregistrées")

    # Velib
    velib_data = fetch_velib_data()
    velib_rows = transform_velib_data(velib_data)

    velib_schema = StructType() \
        .add("stationcode", StringType()) \
        .add("num_bikes_available", IntegerType()) \
        .add("num_docks_available", IntegerType()) \
        .add("is_installed", BooleanType()) \
        .add("is_renting", BooleanType()) \
        .add("is_returning", BooleanType()) \
        .add("last_reported", LongType()) \
        .add("timestamp", StringType())

    velib_df = spark.createDataFrame(velib_rows, schema=velib_schema)
    velib_df.write.mode("append").parquet("hdfs://namenode:9000/velib/raw/availability")
    print("✅ Données Velib enregistrées")

    spark.stop()

if __name__ == "__main__":
    main()
