from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, when, to_timestamp, trim


# ========== AVAILABILITY CLEANING ==========

def read_availability_data(spark: SparkSession) -> DataFrame:
    df = spark.read.parquet("hdfs://namenode:9000/velib/raw/availability")
    print(f"[Availability] Raw rows read: {df.count()}")
    df.show(5, truncate=False)
    return df


def clean_stationcode(df: DataFrame) -> DataFrame:
    return df.withColumn("stationcode", trim(col("stationcode").cast("string")))


def clean_geolocation(df: DataFrame) -> DataFrame:
    df = df.withColumn("lat", when(col("lat") == "\\N", None).otherwise(col("lat").cast("double"))) \
           .withColumn("lon", when(col("lon") == "\\N", None).otherwise(col("lon").cast("double")))
    print(f"[Availability] After cast lat/lon: {df.count()}")
    return df


def filter_numeric_columns(df: DataFrame) -> DataFrame:
    df = df.filter(
        (col("num_bikes_available") >= 0) &
        (col("num_docks_available") >= 0) &
        (col("mechanical") >= 0) &
        (col("ebike") >= 0)
    )
    print(f"[Availability] After numeric filters: {df.count()}")
    return df


def validate_bike_consistency(df: DataFrame) -> DataFrame:
    df = df.withColumn("bike_check", col("mechanical") + col("ebike"))
    df = df.filter(col("bike_check") == col("num_bikes_available"))
    print(f"[Availability] After bike consistency check: {df.count()}")
    return df.drop("bike_check")


def convert_timestamp(df: DataFrame) -> DataFrame:
    df = df.withColumn("event_ts", to_timestamp(col("last_reported"), "yyyy-MM-dd'T'HH:mm:ssXXX"))
    df.select("last_reported", "event_ts").show(5, truncate=False)
    return df


def deduplicate_availability(df: DataFrame) -> DataFrame:
    df = df.dropDuplicates(["stationcode", "timestamp"])
    print(f"[Availability] After deduplication: {df.count()}")
    return df


def write_clean_availability(df: DataFrame) -> None:
    df.write.mode("overwrite").parquet("hdfs://namenode:9000/velib/clean/availability")
    print("[Availability] ✅ Clean data written to HDFS.")


def clean_availability_data(spark: SparkSession) -> None:
    df = read_availability_data(spark)
    print(f"[cleanData] Nombre de lignes avant nettoyage: {df.count()}")
    df = clean_stationcode(df)
    df = clean_geolocation(df)
    df = filter_numeric_columns(df)
    df = validate_bike_consistency(df)
    df = convert_timestamp(df)
    df = deduplicate_availability(df)
    print(f"[cleanData] Nombre de lignes après nettoyage: {df.count()}")
    write_clean_availability(df)


# ========== STATION CLEANING ==========

def read_station_data(spark: SparkSession) -> DataFrame:
    df = spark.read.parquet("hdfs://namenode:9000/velib/raw/stations")
    print(f"[Stations] Raw rows read: {df.count()}")
    df.show(5, truncate=False)
    return df


def filter_valid_geolocation(df: DataFrame) -> DataFrame:
    df = df.filter((col("lat").isNotNull()) & (col("lon").isNotNull()))
    print(f"[Stations] After filtering geolocation: {df.count()}")
    return df


def deduplicate_stations(df: DataFrame) -> DataFrame:
    df = df.dropDuplicates(["stationcode"])
    print(f"[Stations] After deduplication: {df.count()}")
    return df


def write_clean_stations(df: DataFrame) -> None:
    df.write.mode("overwrite").parquet("hdfs://namenode:9000/velib/clean/stations")
    print("[Stations] ✅ Clean data written to HDFS.")


def clean_station_data(spark: SparkSession) -> None:
    df = read_station_data(spark)
    df = clean_stationcode(df)
    df = filter_valid_geolocation(df)
    df = deduplicate_stations(df)
    write_clean_stations(df)


# ========== MAIN ==========

def main():
    spark = SparkSession.builder.appName("CleanVelibData").getOrCreate()
    print("[cleanData] 🚀 Démarrage du nettoyage")

    print("[cleanData] --- Nettoyage AVAILABILITY ---")
    clean_availability_data(spark)

    print("[cleanData] --- Nettoyage STATIONS ---")
    clean_station_data(spark)

    spark.stop()
    print("[cleanData] ✅ Nettoyage terminé avec succès")


if __name__ == "__main__":
    main()
