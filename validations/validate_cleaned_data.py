from pyspark.sql import SparkSession
from pyspark.sql.functions import col


def validate_clean_availability(spark):
    print("🔎 Validation des données cleanées : availability")
    df = spark.read.parquet("hdfs://namenode:9000/velib/clean/availability")
    df.cache()
    errors = []

    # 1. stationcode
    if df.filter(col("stationcode").isNull() | (col("stationcode") == "")).count() > 0:
        errors.append("❌ availability.stationcode contient des valeurs nulles ou vides.")

    # 2. Geoloc
    if df.filter(col("lat").isNull() | col("lon").isNull()).count() > 0:
        errors.append("❌ availability contient des coordonnées nulles.")

    # 3. Valeurs numériques
    numeric_columns = ["num_bikes_available", "num_docks_available", "mechanical", "ebike"]
    for colname in numeric_columns:
        if df.filter((col(colname).isNull()) | (col(colname) < 0)).count() > 0:
            errors.append(f"❌ {colname} contient des valeurs nulles ou < 0.")

    # 4. Cohérence des vélos
    if df.filter(col("mechanical") + col("ebike") != col("num_bikes_available")).count() > 0:
        errors.append("❌ Incohérence entre mechanical + ebike et num_bikes_available.")

    # 5. timestamp
    if df.filter(col("event_ts").isNull()).count() > 0:
        errors.append("❌ event_ts est nul pour certaines lignes.")

    # 6. Unicité stationcode + timestamp
    if df.count() != df.select("stationcode", "timestamp").dropDuplicates().count():
        errors.append("❌ Doublons trouvés sur stationcode + timestamp dans availability.")

    if errors:
        print("❌ Erreurs de validation (availability) :")
        for err in errors:
            print(err)
        raise Exception("Validations availability échouées.")
    else:
        print("✅ Données availability cleanées valides !")


def validate_clean_stations(spark):
    print("🔎 Validation des données cleanées : stations")
    df = spark.read.parquet("hdfs://namenode:9000/velib/clean/stations")
    df.cache()
    errors = []

    # 1. stationcode
    if df.filter(col("stationcode").isNull() | (col("stationcode") == "")).count() > 0:
        errors.append("❌ stations.stationcode contient des valeurs nulles ou vides.")

    # 2. Coordonnées
    if df.filter(col("lat").isNull() | col("lon").isNull()).count() > 0:
        errors.append("❌ stations contient des coordonnées nulles.")

    # 3. Doublons sur stationcode
    if df.count() != df.select("stationcode").dropDuplicates().count():
        errors.append("❌ Doublons trouvés sur stationcode dans stations.")

    if errors:
        print("❌ Erreurs de validation (stations) :")
        for err in errors:
            print(err)
        raise Exception("Validations stations échouées.")
    else:
        print("✅ Données stations cleanées valides !")


def main():
    spark = SparkSession.builder.appName("ValidateCleanedVelib").getOrCreate()
    validate_clean_availability(spark)
    validate_clean_stations(spark)
    spark.stop()


if __name__ == "__main__":
    main()
