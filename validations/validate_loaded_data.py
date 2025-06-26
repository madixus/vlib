from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def read_parquet(spark: SparkSession, path: str):
    try:
        df = spark.read.parquet(path)
        print(f"[VALIDATION] 📁 Données lues depuis {path} — {df.count()} lignes.")
        return df
    except Exception as e:
        print(f"[VALIDATION] ❌ Erreur de lecture du fichier : {e}")
        return None

def validate_schema(source_df, target_df):
    source_schema = set((f.name, f.dataType) for f in source_df.schema.fields)
    target_schema = set((f.name, f.dataType) for f in target_df.schema.fields)

    if source_schema != target_schema:
        print("[VALIDATION] ❌ Schémas différents entre les datasets.")
        print("Différence :", source_schema.symmetric_difference(target_schema))
    else:
        print("[VALIDATION] ✅ Schéma conforme.")

def validate_not_null(df, columns: list[str]):
    for col_name in columns:
        null_count = df.filter(col(col_name).isNull()).count()
        if null_count > 0:
            print(f"[VALIDATION] ⚠️ Colonne '{col_name}' contient {null_count} valeurs nulles.")
        else:
            print(f"[VALIDATION] ✅ Colonne '{col_name}' : aucune valeur nulle.")

def main():
    spark = SparkSession.builder.appName("ValidateLoadedVelibData").getOrCreate()

    # Lire les datasets
    aggregated_df = read_parquet(spark, "hdfs://namenode:9000/velib/aggregation/data")
    final_df = read_parquet(spark, "hdfs://namenode:9000/velib/final/data")

    if aggregated_df and final_df:
        validate_schema(aggregated_df, final_df)

        print(f"[VALIDATION] ✅ Nombre total dans final/data : {final_df.count()} lignes")

        # Vérifier certaines colonnes critiques
        validate_not_null(final_df, ["stationcode", "event_ts", "aggregation_timestamp"])

        print("[VALIDATION] 🧪 Validation terminée.")

    spark.stop()

if __name__ == "__main__":
    main()
