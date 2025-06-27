from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct

def read_parquet_data(spark: SparkSession, path: str):
    try:
        df = spark.read.parquet(path)
        print(f"[VALIDATION] 📦 Données source lues depuis {path} — {df.count()} lignes.")
        return df
    except Exception as e:
        print(f"[VALIDATION] ❌ Erreur de lecture : {e}")
        return None

def validate_not_null(df, columns: list[str]):
    for col_name in columns:
        null_count = df.filter(col(col_name).isNull()).count()
        if null_count > 0:
            print(f"[VALIDATION] ⚠️ Colonne '{col_name}' contient {null_count} valeurs nulles.")
        else:
            print(f"[VALIDATION] ✅ Colonne '{col_name}' : aucune valeur nulle.")

def validate_uniqueness(df, cols: list[str]):
    total = df.count()
    distinct = df.select(cols).dropDuplicates().count()
    if total != distinct:
        print(f"[VALIDATION] ⚠️ Unicité non respectée pour les colonnes {cols}. ({distinct}/{total} uniques)")
    else:
        print(f"[VALIDATION] ✅ Unicité respectée sur {cols}.")

def validate_columns_presence(df, required_cols: list[str]):
    missing = [col for col in required_cols if col not in df.columns]
    if missing:
        print(f"[VALIDATION] ❌ Colonnes manquantes : {missing}")
    else:
        print(f"[VALIDATION] ✅ Toutes les colonnes attendues sont présentes.")

def main():
    spark = SparkSession.builder.appName("ValidateVelibPostgresJob").getOrCreate()

    path = "hdfs://namenode:9000/velib/final/data"
    df = read_parquet_data(spark, path)

    if df is not None:
        # Vérification de colonnes essentielles
        critical_cols = ["stationcode", "event_ts", "num_bikes_available", "capacity", "aggregation_timestamp"]
        validate_columns_presence(df, critical_cols)
        validate_not_null(df, critical_cols)

        # Unicité pour les faits (clé composite)
        validate_uniqueness(df, ["event_ts", "stationcode"])

        # Unicité des stations
        validate_uniqueness(df.select("stationcode", "name", "lat", "lon"), ["stationcode"])

        # Unicité du temps
        unique_timestamps = df.select("event_ts").dropDuplicates().count()
        print(f"[VALIDATION] 📊 {unique_timestamps} timestamps uniques détectés.")

        print("[VALIDATION] 🧪 Validation terminée.")

    spark.stop()

if __name__ == "__main__":
    main()
