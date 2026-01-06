from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import to_date, col, desc, asc, dense_rank, avg
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, DateType

spark = SparkSession.builder.appName("log_analytics").getOrCreate()

logs_schema = StructType(
    [
        StructField("timestamp", DateType(), False),
        StructField("ip", StringType(), False),
        StructField("endpoint", StringType(), False),
        StructField("response_codes", IntegerType(), False),
        StructField("response_time", IntegerType(), False),
    ]
)

logs_df = spark.read.schema(logs_schema).csv("data/my_logs.csv", header=True)
# create_append_logs(1000000)

""" Top 10 most visited endpoints per day """
logs_with_date = logs_df.withColumn("date", to_date(col("timestamp")))
# logs_with_date.show()

logs_with_endpoint_count = logs_with_date.groupBy("date", "endpoint").count()
wind_spec = Window.partitionBy("date").orderBy(col("count").desc())
resultant_df = logs_with_endpoint_count.withColumn(
    "rank", dense_rank().over(wind_spec)
).filter(col("rank") <= 10)
resultant_df.show()
# resultant_df.show(n=resultant_df.count())


""" Average & P95 response time per endpoint """
logs_df.groupBy("endpoint").agg(
    avg(col("response_time")).alias("avg_response_time")
).show()


""" Detect IPs making >1000 requests/hour """
