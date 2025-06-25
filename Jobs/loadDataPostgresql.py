import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, dayofmonth, hour, minute, second

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("loadDataPostgresql")

def main():
    logger.info("🚀 Job Spark: loadDataPostgresql")

    spark = SparkSession.builder \
        .appName("Load Velib Data to PostgreSQL") \
        .config("spark.jars", "/extra-jars/postgresql-42.7.5.jar") \
        .getOrCreate()

    input_path = "hdfs://namenode:9000/velib/final/data"
    df_raw = spark.read.parquet(input_path)
    logger.info(f"📦 {df_raw.count()} lignes lues depuis HDFS")

    # === Connexion PostgreSQL
    url = "jdbc:postgresql://postgres:5432/vlib"
    props = {"user": "vlib", "password": "vlib", "driver": "org.postgresql.Driver"}

    # === Filtrage des lignes déjà insérées dans dim_time
    try:
        logger.info("🔍 Lecture des clés existantes dans dim_time")
        dim_time_pg = spark.read.jdbc(url=url, table="dim_time", properties=props).select("event_ts")
        event_ts_existants = [row["event_ts"] for row in dim_time_pg.collect()]
        df_raw = df_raw.filter(~col("event_ts").isin(event_ts_existants))
        logger.info(f"✅ Filtrage : {df_raw.count()} lignes restantes après exclusion des doublons")
    except Exception as e:
        logger.warning(f"⚠️ Impossible de lire dim_time : {e}. Insertion complète prévue.")

    # === Tables transformées
    dim_station = df_raw.select(
        "stationcode", "name", "lat", "lon", "arrondissement", "capacity", "station_opening_hours"
    ).dropDuplicates(["stationcode"])

    dim_time = df_raw.select("event_ts").dropDuplicates() \
        .withColumn("year", year("event_ts")) \
        .withColumn("month", month("event_ts")) \
        .withColumn("day", dayofmonth("event_ts")) \
        .withColumn("hour", hour("event_ts")) \
        .withColumn("minute", minute("event_ts")) \
        .withColumn("second", second("event_ts"))

    fact_velib = df_raw.select(
        "event_ts", "stationcode", "num_bikes_available", "num_docks_available",
        "mechanical", "ebike", "is_installed", "is_renting", "is_returning",
        "capacity", "aggregation_timestamp"
    ).dropDuplicates(["event_ts", "stationcode"])

    logger.info(f"🛠 fact_velib générée : {fact_velib.count()} lignes")

    # === Écritures conditionnelles
    try:
        logger.info("💾 dim_station → PostgreSQL")
        dim_station.write.jdbc(url=url, table="dim_station", mode="append", properties=props)
    except Exception as e:
        logger.warning(f"⚠️ dim_station non insérée : {e}")

    try:
        logger.info("💾 dim_time → PostgreSQL")
        dim_time.write.jdbc(url=url, table="dim_time", mode="append", properties=props)
    except Exception as e:
        logger.warning(f"⚠️ dim_time non insérée : {e}")

    try:
        logger.info("💾 fact_velib → PostgreSQL")
        fact_velib.write.jdbc(url=url, table="fact_velib", mode="append", properties=props)
    except Exception as e:
        logger.error(f"❌ fact_velib non insérée : {e}")

    spark.stop()
    logger.info("🏁 Fin du job Spark")

if __name__ == "__main__":
    main()
