from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def run_validations():
    spark = SparkSession.builder.appName("ValidateStations").getOrCreate()
    df = spark.read.parquet("hdfs://namenode:9000/velib/raw/stations")
    df.cache()

    errors = []

    # 1. stationcode
    if df.filter(col("stationcode").isNull() | (col("stationcode") == "")).count() > 0:
        errors.append("❌ stationcode contient des valeurs nulles ou vides.")

    # 2. name
    if df.filter(col("name").isNull() | (col("name") == "")).count() > 0:
        errors.append("❌ name contient des valeurs nulles ou vides.")

    # 3. capacity
    capacity_invalid = df.filter((col("capacity").isNull()) | (col("capacity") <= 0))
    if capacity_invalid.count() > 0:
        errors.append("❌ Certaines capacités sont nulles ou ≤ 0.")
        print("🔍 Lignes avec capacity nulle ou ≤ 0 :")
        capacity_invalid.select("stationcode", "name", "capacity").show(truncate=False) 

    # 4. Coordonnées
    if df.filter((col("lat").isNull()) | (col("lat") < -90) | (col("lat") > 90)).count() > 0:
        errors.append("❌ Latitude invalide ou manquante.")
    if df.filter((col("lon").isNull()) | (col("lon") < -180) | (col("lon") > 180)).count() > 0:
        errors.append("❌ Longitude invalide ou manquante.")

    # 5. timestamp
    if df.filter(col("timestamp").isNull()).count() > 0:
        errors.append("❌ timestamp contient des valeurs nulles.")

    # 6. Minimum de lignes
    if df.count() == 0:
        errors.append("❌ Le fichier ne contient aucune station.")

    print(f"🔎 Nombre total de stations : {df.count()}")

    if errors:
        print("❌ Erreurs de validation stations :")
        for err in errors:
            print(err)
        raise Exception("Validations échouées.")
    else:
        print("✅ Toutes les validations stations ont réussi.")

    spark.stop()

if __name__ == "__main__":
    run_validations()
