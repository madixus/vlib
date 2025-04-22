from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

def main():
    spark = SparkSession.builder.appName("LoadVelibFinalData").getOrCreate()

    # Lire les données nettoyées depuis HDFS
    df = spark.read.parquet("hdfs://namenode:9000/velib/clean/data")

    # Ajouter une colonne de timestamp de chargement (optionnel mais conseillé)
    df = df.withColumn("load_ts", current_timestamp())

    # Écriture en append pour garder tout l’historique
    df.write.mode("append").parquet("hdfs://namenode:9000/velib/final/full_history")
    print("✅ Données chargées dans l'historique final (/final/full_history)")

    spark.stop()

if __name__ == "__main__":
    main()
