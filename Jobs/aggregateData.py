from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, to_timestamp, row_number
from pyspark.sql.window import Window
from pyspark.sql.functions import current_timestamp

def main():
    spark = SparkSession.builder.appName("JoinVelibData").getOrCreate()

    # Lecture des données nettoyées
    avail_df = spark.read.parquet("hdfs://namenode:9000/velib/clean/availability")
    station_df = spark.read.parquet("hdfs://namenode:9000/velib/clean/stations")

    # Nettoyage des colonnes 'stationcode'
    avail_df = avail_df.withColumn("stationcode", trim(col("stationcode").cast("string")))
    station_df = station_df.withColumn("stationcode", trim(col("stationcode").cast("string")))

    # Conversion des timestamps
    avail_df = avail_df.withColumn("event_ts", to_timestamp("event_ts"))
    station_df = station_df.withColumn("timestamp", to_timestamp("timestamp"))

    # Sélection des colonnes utiles
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

    # Récupérer le dernier état connu de chaque station
    window_spec = Window.partitionBy("stationcode").orderBy(col("timestamp").desc())
    station_latest = station_df.withColumn("rn", row_number().over(window_spec))\
                               .filter(col("rn") == 1)\
                               .drop("rn", "timestamp")  # on peut drop le timestamp historique

    # Jointure uniquement sur stationcode
    joined_df = avail_df.join(station_latest, on="stationcode", how="left")

    
    joined_df = joined_df.withColumn("aggregation_timestamp", current_timestamp())

    print(f"[joinData] Données jointes : {joined_df.count()}")
    joined_df.show(5, truncate=False)

    # Sauvegarde en Parquet et CSV
    joined_df.write.mode("overwrite").parquet("hdfs://namenode:9000/velib/aggregation/data")
    print("[joinData] Données enrichies enregistrées en Parquet")

    joined_df.write.mode("overwrite").option("header", True).csv("hdfs://namenode:9000/velib/aggregation/csvfile")
    print("[joinData] Données enrichies enregistrées en CSV")
    print("[DEBUG] Aperçu des 20 lignes complètes après jointure :")
    joined_df.show(20, truncate=False)

    spark.stop()
    print("[joinData] Traitement terminé")


if __name__ == "__main__":
    main()
