from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, trim, to_timestamp, row_number, current_timestamp
from pyspark.sql.window import Window


def read_clean_data(spark: SparkSession) -> tuple[DataFrame, DataFrame]:
    avail_df = spark.read.parquet("hdfs://namenode:9000/velib/clean/availability")
    station_df = spark.read.parquet("hdfs://namenode:9000/velib/clean/stations")
    return avail_df, station_df


def clean_columns(avail_df: DataFrame, station_df: DataFrame) -> tuple[DataFrame, DataFrame]:
    avail_df = avail_df.withColumn("stationcode", trim(col("stationcode").cast("string")))
    station_df = station_df.withColumn("stationcode", trim(col("stationcode").cast("string")))

    avail_df = avail_df.withColumn("event_ts", to_timestamp("event_ts"))
    station_df = station_df.withColumn("timestamp", to_timestamp("timestamp"))

    return avail_df, station_df


def select_useful_columns(avail_df: DataFrame, station_df: DataFrame) -> tuple[DataFrame, DataFrame]:
    avail_df = avail_df.select(
        "stationcode", "event_ts",
        "num_bikes_available", "num_docks_available",
        "mechanical", "ebike",
        "is_installed", "is_renting", "is_returning",
        "last_reported", "arrondissement"
    )

    station_df = station_df.select(
        "stationcode", "timestamp",
        "name", "capacity", "lat", "lon", "station_opening_hours"
    )
    return avail_df, station_df


def get_latest_station_info(station_df: DataFrame) -> DataFrame:
    window_spec = Window.partitionBy("stationcode").orderBy(col("timestamp").desc())

    station_latest = (
        station_df.withColumn("rn", row_number().over(window_spec))
                  .filter(col("rn") == 1)
                  .drop("rn", "timestamp")
    )
    return station_latest


def join_and_enrich(avail_df: DataFrame, station_df: DataFrame) -> DataFrame:
    joined_df = avail_df.join(station_df, on="stationcode", how="left")
    joined_df = joined_df.withColumn("aggregation_timestamp", current_timestamp())
    return joined_df


def write_joined_data(joined_df: DataFrame) -> None:
    joined_df.write.mode("overwrite").parquet("hdfs://namenode:9000/velib/aggregation/data")
    print("[joinData] Données enrichies enregistrées en Parquet")

    joined_df.write.mode("overwrite") \
        .option("header", True) \
        .csv("hdfs://namenode:9000/velib/aggregation/csvfile")
    print("[joinData] Données enrichies enregistrées en CSV")


def aggregate_data(spark: SparkSession) -> None:
    avail_df, station_df = read_clean_data(spark)

    avail_df, station_df = clean_columns(avail_df, station_df)
    avail_df, station_df = select_useful_columns(avail_df, station_df)
    station_latest = get_latest_station_info(station_df)

    joined_df = join_and_enrich(avail_df, station_latest)

    print(f"[joinData] Données jointes : {joined_df.count()}")
    joined_df.show(5, truncate=False)

    write_joined_data(joined_df)

    print("[DEBUG] Aperçu des 20 lignes complètes après jointure :")
    joined_df.show(20, truncate=False)


def main() -> None:
    spark = SparkSession.builder.appName("JoinVelibData").getOrCreate()
    aggregate_data(spark)
    spark.stop()
    print("[joinData] Traitement terminé")


if __name__ == "__main__":
    main()
