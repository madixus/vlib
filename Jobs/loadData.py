# === loadData.py avec logs ===
from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder.appName("LoadVelibData").getOrCreate()

    # Lire les données agrégées depuis HDFS
    
    input_path = "hdfs://namenode:9000/velib/aggregation/data"
    df = spark.read.parquet(input_path)
    print(f"[loadData] Données agrégées lues : {df.count()}")
    df.show(5)

    # Écrire les données dans un répertoire "final"
    output_path = "hdfs://namenode:9000/velib/final/data"
    df.write.mode("append").parquet(output_path)
    print("\n✅ Données finales enregistrées dans HDFS.")

    spark.stop()

if __name__ == "__main__":
    main()