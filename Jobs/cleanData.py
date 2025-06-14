from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, to_timestamp, trim

def clean_availability_data(spark):
    df = spark.read.parquet("hdfs://namenode:9000/velib/raw/availability_v2")
    print(f"[cleanData] Availability - lignes lues : {df.count()}")
    df.show(5, truncate=False)

    # Nettoyage stationcode
    df = df.withColumn("stationcode", trim(col("stationcode").cast("string")))

    # Nettoyage géolocalisation
    df = df.withColumn("lat", when(col("lat") == "\\N", None).otherwise(col("lat").cast("double")))
    df = df.withColumn("lon", when(col("lon") == "\\N", None).otherwise(col("lon").cast("double")))
    print(f"[cleanData] Availability - après cast lat/lon : {df.count()}")

    # df = df.filter((col("lat").isNotNull()) & (col("lon").isNotNull()))
    print(f"[cleanData] Availability - après filtre géoloc : {df.count()}")

    # Filtres numériques
    df = df.filter(
        (col("num_bikes_available") >= 0) &
        (col("num_docks_available") >= 0) &
        (col("mechanical") >= 0) &
        (col("ebike") >= 0)
    )
    print(f"[cleanData] Availability - après filtres numériques : {df.count()}")

    # Vérification cohérence vélos
    df = df.withColumn("bike_check", col("mechanical") + col("ebike"))
    df = df.filter(col("bike_check") == col("num_bikes_available"))
    print(f"[cleanData] Availability - après vérif cohérence vélo : {df.count()}")

    # Conversion horodatage
    df = df.withColumn("event_ts", to_timestamp(col("last_reported"), "yyyy-MM-dd'T'HH:mm:ssXXX"))
    df.select("last_reported", "event_ts").show(5, truncate=False)

    # Déduplication
    df = df.dropDuplicates(["stationcode", "timestamp"])
    print(f"[cleanData] Availability - après suppression doublons : {df.count()}")

    # Sauvegarde
    df.write.mode("overwrite").parquet("hdfs://namenode:9000/velib/clean/availability")
    print("[cleanData] Availability - données nettoyées enregistrées")

def clean_station_data(spark):
    df = spark.read.parquet("hdfs://namenode:9000/velib/raw/stations")
    print(f"[cleanData] Stations - lignes lues : {df.count()}")
    df.show(5, truncate=False)

    # Nettoyage stationcode
    df = df.withColumn("stationcode", trim(col("stationcode").cast("string")))

    # Filtrage géolocalisation
    df = df.filter((col("lat").isNotNull()) & (col("lon").isNotNull()))
    print(f"[cleanData] Stations - après filtre géoloc : {df.count()}")

    # Déduplication
    df = df.dropDuplicates(["stationcode"])
    print(f"[cleanData] Stations - après suppression doublons : {df.count()}")

    # Sauvegarde
    df.write.mode("overwrite").parquet("hdfs://namenode:9000/velib/clean/stations")
    print("[cleanData] Stations - données nettoyées enregistrées")

def main():
    spark = SparkSession.builder.appName("CleanVelibData").getOrCreate()
    print("[cleanData] --- Démarrage nettoyage disponibilité ---")
    clean_availability_data(spark)
    print("[cleanData] --- Démarrage nettoyage stations ---")
    clean_station_data(spark)
    spark.stop()
    print("[cleanData] Nettoyage terminé")

if __name__ == "__main__":
    main()
