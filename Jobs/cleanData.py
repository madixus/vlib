from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, to_timestamp

def main():
    spark = SparkSession.builder.appName("CleanVelibData").getOrCreate()

    # ========== 🗂️ LECTURE DES DONNÉES BRUTES ==========
    availability_df = spark.read.parquet("hdfs://namenode:9000/velib/raw/availability_v2")
    stations_df = spark.read.parquet("hdfs://namenode:9000/velib/raw/stations")

    # ========== 🔗 JOINTURE ==========
    df = availability_df.join(stations_df, on="stationcode", how="left")

    # ========== 🧹 NETTOYAGE ==========
    # 1. Supprimer les lignes sans coordonnées
    df = df.filter(col("lat").isNotNull() & col("lon").isNotNull())

    # 2. Supprimer les valeurs incohérentes
    df = df.filter(
        (col("num_bikes_available") >= 0) &
        (col("num_docks_available") >= 0) &
        (col("mechanical") >= 0) &
        (col("ebike") >= 0)
    )

    # 3. Vérifier que mechanical + ebike == total bikes
    df = df.withColumn("bike_check", col("mechanical") + col("ebike"))
    df = df.filter(col("bike_check") == col("num_bikes_available"))

    # 4. Supprimer les stations avec capacity = 0 ou null
    df = df.filter(col("capacity").isNotNull() & (col("capacity") > 0))

    # 5. Supprimer les doublons (par station et timestamp)
    df = df.dropDuplicates(["stationcode", "ingestion_ts"])

    # 6. Convertir last_reported en timestamp
    df = df.withColumn("event_ts", to_timestamp(col("last_reported")))

    # ========== ✨ ENRICHISSEMENT ==========
    df = df.withColumn("is_full_station", when(col("num_docks_available") == 0, True).otherwise(False))
    df = df.withColumn("is_empty_station", when(col("num_bikes_available") == 0, True).otherwise(False))

    # ========== 💾 ÉCRITURE ==========
    df.write.mode("overwrite").parquet("hdfs://namenode:9000/velib/clean/data")
    print("✅ Données nettoyées et enrichies écrites avec succès !")

    spark.stop()

if __name__ == "__main__":
    main()
