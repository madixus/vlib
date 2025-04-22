from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, hour

def main():
    spark = SparkSession.builder.appName("AggregateVelibRawData").getOrCreate()

    df = spark.read.parquet("hdfs://namenode:9000/velib/clean/data")

    # Ajouter des colonnes pour filtrage temporel plus facile dans le dashboard
    df = df.withColumn("event_date", to_date("event_ts")) \
           .withColumn("event_hour", hour("event_ts"))

    # Sélection des colonnes utiles
    selected_df = df.select(
        "stationcode",
        "name",
        "arrondissement",
        "capacity",
        "num_bikes_available",
        "num_docks_available",
        "mechanical",
        "ebike",
        "is_full_station",
        "is_empty_station",
        "event_ts",
        "event_date",
        "event_hour"
    )

    # Écriture sans agrégation — toutes les données par timestamp
    selected_df.write.mode("overwrite").parquet("hdfs://namenode:9000/velib/dashboard/raw_timeseries")
    print(" Données brutes agrégées écrites avec succès !")

    spark.stop()

if __name__ == "__main__":
    main()
