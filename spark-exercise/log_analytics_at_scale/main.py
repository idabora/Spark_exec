import os
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import to_date, col, desc, asc, dense_rank
from random_data_generator import (
    random_timstamps,
    generate_endpoint_and_resp_codes,
    generate_ip,
    generate_response_time,
)

spark = SparkSession.builder.appName("log_analytics").getOrCreate()
# create_append_logs(1000000)

""" Top 10 most visited endpoints per day """
logs_df = spark.read.csv("data/my_logs.csv", header=True)
logs_with_date = logs_df.withColumn("date", to_date(col("timestamp")))
# logs_with_date.show()

logs_with_endpoint_count = logs_with_date.groupBy("date", "endpoint").count()
wind_spec = Window.partitionBy("date").orderBy(col("count").desc())
resultant_df = logs_with_endpoint_count.withColumn(
    "rank", dense_rank().over(wind_spec)
).filter(col("rank") <= 10)
resultant_df.show(n=resultant_df.count())


""" Average & P95 response time per endpoint """

""" Detect IPs making >1000 requests/hour """
