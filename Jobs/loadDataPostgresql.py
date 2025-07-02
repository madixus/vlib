import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, dayofmonth, hour, minute, second

# === Configuration logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("loadDataPostgresql")

# === Initialisation Spark
def create_spark_session():
    print("Fonction create_spark_session exécutée")
    return SparkSession.builder \
        .appName("Load Velib Data to PostgreSQL") \
        .config("spark.jars", "/extra-jars/postgresql-42.7.5.jar") \
        .getOrCreate()

# === Lecture des données source HDFS
def read_source_data(spark: SparkSession, input_path: str):
    df = spark.read.parquet(input_path)
    logger.info(f"📦 {df.count()} lignes lues depuis HDFS")
    return df

# === Connexion PostgreSQL + filtrage des doublons
def filter_existing_timestamps(spark: SparkSession, df_raw, url: str, props: dict):
    try:
        logger.info("🔍 Lecture des clés existantes dans dim_time")
        dim_time_pg = spark.read.jdbc(url=url, table="dim_time", properties=props).select("event_ts")
        existing_timestamps = [row["event_ts"] for row in dim_time_pg.collect()]
        df_filtered = df_raw.filter(~col("event_ts").isin(existing_timestamps))
        logger.info(f"✅ Filtrage : {df_filtered.count()} lignes restantes après exclusion des doublons")
        return df_filtered
    except Exception as e:
        logger.warning(f"⚠️ Impossible de lire dim_time : {e}. Insertion complète prévue.")
        return df_raw

# === Génération des DataFrames transformés
def create_dim_station(df):
    return df.select(
        "stationcode", "name", "lat", "lon", "arrondissement", "capacity", "station_opening_hours"
    ).dropDuplicates(["stationcode"])

def create_dim_time(df):
    return df.select("event_ts").dropDuplicates() \
        .withColumn("year", year("event_ts")) \
        .withColumn("month", month("event_ts")) \
        .withColumn("day", dayofmonth("event_ts")) \
        .withColumn("hour", hour("event_ts")) \
        .withColumn("minute", minute("event_ts")) \
        .withColumn("second", second("event_ts"))

def create_fact_velib(df):
    fact_df = df.select(
        "event_ts", "stationcode", "num_bikes_available", "num_docks_available",
        "mechanical", "ebike", "is_installed", "is_renting", "is_returning",
        "capacity", "aggregation_timestamp"
    ).dropDuplicates(["event_ts", "stationcode"])
    logger.info(f"🛠 fact_velib générée : {fact_df.count()} lignes")
    return fact_df

# === Écriture PostgreSQL avec gestion des erreurs
def write_to_postgres(df, url, table_name, props):
    try:
        logger.info(f"💾 {table_name} → PostgreSQL")
        df.write.jdbc(url=url, table=table_name, mode="append", properties=props)
    except Exception as e:
        logger.warning(f"⚠️ {table_name} non insérée : {e}")

# === Main du job
def main():
    logger.info("🚀 Job Spark: loadDataPostgresql")

    spark = create_spark_session()

    input_path = "hdfs://namenode:9000/velib/final/data"
    df_raw = read_source_data(spark, input_path)

    url = "jdbc:postgresql://postgres:5432/vlib"
    props = {"user": "vlib", "password": "vlib", "driver": "org.postgresql.Driver"}

    df_filtered = filter_existing_timestamps(spark, df_raw, url, props)

    dim_station = create_dim_station(df_filtered)
    dim_time = create_dim_time(df_filtered)
    fact_velib = create_fact_velib(df_filtered)

    write_to_postgres(dim_station, url, "dim_station", props)
    write_to_postgres(dim_time, url, "dim_time", props)
    write_to_postgres(fact_velib, url, "fact_velib", props)

    spark.stop()
    logger.info("🏁 Fin du job Spark")

if __name__ == "__main__":
    main()
