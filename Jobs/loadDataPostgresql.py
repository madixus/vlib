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

    # Couper les logs Spark trop verbeux
    spark.sparkContext.setLogLevel("WARN")

    input_path = "hdfs://namenode:9000/velib/final/data"
    df_raw = spark.read.parquet(input_path)
    logger.info(f"📦 {df_raw.count()} lignes lues depuis HDFS")

    # === Connexion PostgreSQL ===
    url = "jdbc:postgresql://postgres:5432/vlib"
    props = {"user": "vlib", "password": "vlib", "driver": "org.postgresql.Driver"}

    # Charger en mémoire les existants
    logger.info("🔍 Lecture des existants en base")
    dim_station_pg = spark.read.jdbc(url=url, table="dim_station", properties=props) \
                             .select("stationcode")
    dim_time_pg    = spark.read.jdbc(url=url, table="dim_time",    properties=props) \
                             .select("event_ts")
    fact_pg        = spark.read.jdbc(url=url, table="fact_velib",  properties=props) \
                             .select("stationcode","event_ts")

    # === Préparer dim_station (nouvelles stations uniquement) ===
    dim_station_new = df_raw.select(
        "stationcode", "name", "lat", "lon", "arrondissement", "capacity", "station_opening_hours"
    ).dropDuplicates(["stationcode"]) \
     .join(dim_station_pg, on="stationcode", how="left_anti")

    logger.info(f"✅ {dim_station_new.count()} nouvelles stations à insérer")

    # === Écrire dim_station avant tout ===
    try:
        logger.info("💾 Insertion dim_station → PostgreSQL")
        dim_station_new.write.jdbc(url=url, table="dim_station", mode="append", properties=props)
    except Exception as e:
        logger.error(f"❌ Erreur inserting dim_station: {e}")

    # === Préparer dim_time (nouveaux timestamps uniquement) ===
    df_time_new = df_raw.select("event_ts") \
        .dropDuplicates() \
        .join(dim_time_pg, on="event_ts", how="left_anti")

    logger.info(f"✅ {df_time_new.count()} nouveaux timestamps à insérer")

    # === Construire la dimension temps ===
    dim_time_new = df_time_new \
        .withColumn("year", year("event_ts")) \
        .withColumn("month", month("event_ts")) \
        .withColumn("day", dayofmonth("event_ts")) \
        .withColumn("hour", hour("event_ts")) \
        .withColumn("minute", minute("event_ts")) \
        .withColumn("second", second("event_ts"))

    # === Écrire dim_time ===
    try:
        logger.info("💾 Insertion dim_time → PostgreSQL")
        dim_time_new.write.jdbc(url=url, table="dim_time", mode="append", properties=props)
    except Exception as e:
        logger.error(f"❌ Erreur inserting dim_time: {e}")

    # === Préparer fact_velib (faits nouveaux et cohérents FK) ===
    fact_velib_new = df_raw.select(
        "stationcode", "event_ts", "num_bikes_available", "num_docks_available",
        "mechanical", "ebike", "is_installed", "is_renting", "is_returning",
        "capacity", "aggregation_timestamp"
    ).dropDuplicates(["stationcode", "event_ts"]) \
     .join(fact_pg, on=["stationcode","event_ts"], how="left_anti")

    logger.info(f"✅ {fact_velib_new.count()} nouveaux faits à insérer")

    # === Écrire fact_velib ===
    try:
        logger.info("💾 Insertion fact_velib → PostgreSQL")
        fact_velib_new.write.jdbc(url=url, table="fact_velib", mode="append", properties=props)
    except Exception as e:
        logger.error(f"❌ Erreur inserting fact_velib: {e}")

    spark.stop()
    logger.info("🏁 Fin du job Spark")

if __name__ == "__main__":
    main()
