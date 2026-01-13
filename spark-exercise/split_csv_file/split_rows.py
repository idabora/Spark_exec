from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("split rows").getOrCreate()

df = spark.read.option("header", True).csv("netflix_users.csv")
df.show()

df.repartition(5).write.option("header", True).mode("overwrite").csv("result/")
