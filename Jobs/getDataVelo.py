from pyspark.sql import SparkSession  # type: ignore
from pyspark.sql.types import StructType, StringType, IntegerType, BooleanType, DoubleType  # type: ignore
import requests  # type: ignore
from datetime import datetime


BASE_URL = "https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/velib-disponibilite-en-temps-reel/records"
LIMIT = 100


def fetch_velib_data(base_url=BASE_URL, limit=LIMIT):
    all_records = []
    offset = 0

    while True:
        url = f"{base_url}?limit={limit}&offset={offset}"
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        results = data.get("results", [])
        all_records.extend(results)

        if len(results) < limit:
            break
        offset += limit

    print(f"Total stations récupérées avec succès : {len(all_records)}")
    return all_records


def to_bool(value):
    return str(value).strip().lower() == "oui"


def transform_velib_data(records, timestamp=None):
    if timestamp is None:
        timestamp = datetime.utcnow().isoformat()

    return [
        {
            "stationcode": r.get("stationcode"),
            "name": r.get("name"),
            "num_bikes_available": r.get("numbikesavailable"),
            "num_docks_available": r.get("numdocksavailable"),
            "mechanical": r.get("mechanical"),
            "ebike": r.get("ebike"),
            "is_installed": to_bool(r.get("is_installed")),
            "is_renting": to_bool(r.get("is_renting")),
            "is_returning": to_bool(r.get("is_returning")),
            "lon": r.get("coordonnees_geo", {}).get("lon") if isinstance(r.get("coordonnees_geo"), dict) else None,
            "lat": r.get("coordonnees_geo", {}).get("lat") if isinstance(r.get("coordonnees_geo"), dict) else None,
            "last_reported": r.get("duedate"),
            "arrondissement": r.get("nom_arrondissement_communes"),
            "capacity": r.get("capacity"),
            "timestamp": timestamp
        }
        for r in records
    ]


def get_velib_schema():
    return StructType() \
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
        .add("capacity", IntegerType()) \
        .add("timestamp", StringType())


def write_to_hdfs(df, path, mode="overwrite"):
    df.write.mode(mode).parquet(path)


def run_job():
    spark = SparkSession.builder.appName("VelibDataIngestion").getOrCreate()
    spark.conf.set("spark.hadoop.dfs.replication", "1")

    try:
        records = fetch_velib_data()
        rows = transform_velib_data(records)
        schema = get_velib_schema()
        df = spark.createDataFrame(rows, schema=schema)
        print(f"[getDataVelo] Nombre de lignes récupérées : {df.count()}")
        df.show(5)
        write_to_hdfs(df, "hdfs://namenode:9000/velib/raw/availability")
        print("✅ Données Vélib écrites avec succès dans HDFS.")
    except Exception as e:
        print(f"❌ Erreur pendant le job Spark : {e}")
    finally:
        spark.stop()


if __name__ == "__main__":
    run_job()
