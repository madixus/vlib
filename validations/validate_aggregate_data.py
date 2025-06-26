from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def run_validations():
    spark = SparkSession.builder.appName("ValidateJoinedData").getOrCreate()
    df = spark.read.parquet("hdfs://namenode:9000/velib/aggregation/data")
    df.cache()

    errors = []

    print(f"🔍 Nombre total de lignes jointes : {df.count()}")

    if df.filter(col("stationcode").isNull() | (col("stationcode") == "")).count() > 0:
        errors.append("❌ stationcode contient des valeurs nulles ou vides.")

    if df.filter(col("event_ts").isNull()).count() > 0:
        errors.append("❌ event_ts contient des valeurs nulles.")

    if df.filter(col("aggregation_timestamp").isNull()).count() > 0:
        errors.append("❌ aggregation_timestamp non généré.")

    if df.filter((col("lat").isNull()) | (col("lon").isNull())).count() > 0:
        errors.append("❌ Coordonnées manquantes après jointure.")

    if df.filter(col("capacity").isNull() | (col("capacity") <= 0)).count() > 0:
        errors.append("❌ Capacités nulles ou non valides après jointure.")

    if df.count() == 0:
        errors.append("❌ La table jointe est vide.")

    if errors:
        print("❌ Erreurs de validation des données jointes :")
        for e in errors:
            print(e)
        raise Exception("Échec de validation des données jointes.")
    else:
        print("✅ Validation des données jointes réussie.")

    spark.stop()

if __name__ == "__main__":
    run_validations()
