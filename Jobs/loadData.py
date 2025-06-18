# === loadData.py ===
from pyspark.sql import SparkSession

def read_aggregated_data(spark: SparkSession):
    input_path = "hdfs://namenode:9000/velib/aggregation/data"
    df = spark.read.parquet(input_path)
    print(f"[loadData] Données agrégées lues : {df.count()}")
    df.show(5)
    return df

def write_final_data(df):
    output_path = "hdfs://namenode:9000/velib/final/data"
    df.write.mode("append").parquet(output_path)
    print("\n✅ Données finales enregistrées dans HDFS.")

def main():
    spark = SparkSession.builder.appName("LoadVelibData").getOrCreate()
    
    df = read_aggregated_data(spark)
    write_final_data(df)

    spark.stop()

if __name__ == "__main__":
    main()
