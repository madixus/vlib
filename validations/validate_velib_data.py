from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max as max_, to_timestamp
from datetime import datetime

def run_validations():
    spark = SparkSession.builder.appName("VelibDataValidation").getOrCreate()

    df = spark.read.parquet("hdfs://namenode:9000/velib/raw/availability")
    df.cache()

    errors = []
    warnings = []

    total_count = df.count()
    print(f"🔍 Nombre total de lignes : {total_count}")

    # 1. stationcode null
    if df.filter(col("stationcode").isNull()).count() > 0:
        errors.append("❌ stationcode contient des valeurs nulles.")

    # 2. capacity ≤ 0
    if df.filter(col("capacity") <= 0).count() > 0:
        errors.append("❌ Certaines capacités sont ≤ 0.")

    # 3. vélos < 0
    if df.filter(col("num_bikes_available") < 0).count() > 0:
        errors.append("❌ Certaines stations ont un nombre de vélos disponible < 0.")

    # 4. latitude invalide ou null => avertissement
    if df.filter((col("lat") < -90) | (col("lat") > 90) | col("lat").isNull()).count() > 0:
        warnings.append("⚠️ Coordonnées latitude invalides ou manquantes (tolérées car issues de la source).")

    # 5. longitude invalide ou null => avertissement
    if df.filter((col("lon") < -180) | (col("lon") > 180) | col("lon").isNull()).count() > 0:
        warnings.append("⚠️ Coordonnées longitude invalides ou manquantes (tolérées car issues de la source).")

    # 6. is_installed null
    if df.filter(col("is_installed").isNull()).count() > 0:
        errors.append("❌ is_installed contient des valeurs nulles.")

    # 7. Bounding box Paris
    out_of_bounds_count = df.filter(
        (col("lat") < 48.6) | (col("lat") > 49.0) |
        (col("lon") < 2.2) | (col("lon") > 2.5)
    ).count()
    if out_of_bounds_count > 0:
        warnings.append(f"⚠️ {out_of_bounds_count} stations sont hors de la zone géographique attendue (Paris).")

    # 8. Total vélos = 0
    total_bikes = df.agg({"num_bikes_available": "sum"}).first()[0]
    if total_bikes == 0:
        errors.append("❌ Aucun vélo disponible : données probablement incorrectes.")

    # 9. Taux de latitudes nulles
    null_lat_ratio = df.filter(col("lat").isNull()).count() / total_count
    if null_lat_ratio > 0.1:
        warnings.append(f"⚠️ {round(null_lat_ratio * 100, 2)}% des latitudes sont nulles.")

    # 10. Vérification timestamp récent
    df = df.withColumn("ts", to_timestamp(col("timestamp")))
    latest_ts = df.select(max_("ts")).first()[0]

    if latest_ts is None:
        errors.append("❌ Aucun timestamp trouvé dans les données.")
    else:
        age_sec = (datetime.utcnow() - latest_ts).total_seconds()
        if age_sec > 3600:
            warnings.append(f"⚠️ Les données ont plus d'une heure ({int(age_sec/60)} min).")

    # 11. Volume minimum attendu
    expected_min = 1400
    if total_count < expected_min * 0.95:
        warnings.append(f"⚠️ Seulement {total_count} stations récupérées, attendu environ {expected_min}.")

    # === Résultat ===
    if errors:
        print("❌ Erreurs bloquantes de validation détectées :")
        for err in errors:
            print(err)
        raise Exception("Validations échouées.")
    else:
        print("✅ Aucune erreur bloquante détectée.")

    if warnings:
        print("\n⚠️ Avertissements détectés :")
        for warn in warnings:
            print(warn)

    spark.stop()

if __name__ == "__main__":
    run_validations()
