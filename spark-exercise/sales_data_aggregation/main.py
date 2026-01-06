from pyspark.sql import SparkSession
from pyspark.sql.types import StructField,StructType,StringType,IntegerType,DecimalType,DateType
from pyspark.sql.functions import col, sum, avg, max
spark = SparkSession.builder.appName("aggregate_sales_data").getOrCreate()


# Creating schema

data_schema = StructType([
    StructField("order_id", IntegerType(), False),
    StructField("product_id", StringType(), False),
    StructField("category", StringType(), False),
    StructField("price", DecimalType(10, 2), False),
    StructField("quantity", IntegerType(), False),
    StructField("date", DateType(), False)
    ])

dataset = spark.read\
                .option("header", "true")\
                .schema(data_schema)\
                .csv("data.csv")


# Total revenue per category
# Average order value per day
# Top-selling product by quantity

# Total Revenue
total_revenue = dataset.withColumn("Revenue", col("price") * col("quantity"))\
                .groupBy("category").agg(
                    sum(col("Revenue").cast(DecimalType(10, 2)))
                    .alias("Total_Revenue_Per_Category"))
total_revenue.show()

# Average order value per day
avg_price = dataset.groupBy(col("date")).agg(avg(col("price")).alias("avg_price"))
avg_price.show()

# Top-selling product by quantity
top_selling_product = dataset.groupBy("product_id").agg(max(col("quantity")).alias("quantity"))
top_selling_product.show(n=top_selling_product.count())
