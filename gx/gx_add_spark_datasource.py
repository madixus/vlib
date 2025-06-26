from great_expectations.data_context import get_context

context = get_context()

context.sources.add_or_update_spark(
    name="spark_hdfs_velib",
    spark_config={
        "spark.master": "spark://spark-master:7077",
        "spark.hadoop.fs.defaultFS": "hdfs://namenode:9000",
    },
)
