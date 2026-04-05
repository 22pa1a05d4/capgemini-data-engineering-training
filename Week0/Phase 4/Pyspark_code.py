from pyspark.sql import SparkSession
from pyspark.sql.functions import col,when,count

spark = SparkSession.builder.appName('Spark Playground').getOrCreate()
customers = spark.read.format('csv').option('header', 'true').load('/samples/customers.csv')
sales = spark.read.format('csv').option('header', 'true').load('/samples/sales.csv')
sales=sales.withColumn("total_amount",col("total_amount").cast("int"))
df = sales.join(customers, "customer_id")

print("1. Daily Sales")
sales.groupBy("sale_date").sum("total_amount").withColumnRenamed("sum(total_amount)","Daily_sales").select("sale_date","Daily_sales").show()
# Display the DataFrame using the display() function.
display(df)

print("2. City-wise Revenue")
sales.join(customers,on="customer_id",how="inner").groupBy("city").sum("total_amount").withColumnRenamed("sum(total_amount)","total_revenue").select("city","total_revenue").show()

print("3. Top 5 Customers")
from pyspark.sql.functions import col, sum, concat_ws
df = df.withColumn("customer_name", concat_ws(" ", col("first_name"), col("last_name")))
df.groupBy("customer_name").agg(sum("total_amount").alias("total_spend")).orderBy(col("total_spend").desc()).limit(5).show()

print("4. Repeat Customers (>1 order)")
sales.groupBy("customer_id").count().filter("count > 1").withColumnRenamed("count", "order_count").show()

print("5.  Customer Segmentation → Create business logic: total_spend > 10000 → Gold 5000–10000 →Silver <5000 → Bronze")
customer_spend = df.groupBy("customer_name").agg(sum("total_amount").alias("total_spend"))
customer_segment = customer_spend.withColumn(
    "segment",
    when(col("total_spend") > 10000, "Gold")
    .when((col("total_spend") >= 5000) & (col("total_spend") <= 10000), "Silver")
    .otherwise("Bronze")
)
customer_segment.show()

print("6. Final Reporting Table → Combine all relevant insights into a final table.")
order_count = sales.groupBy("customer_id").agg(count("*").alias("order_count"))
total_spend = df.groupBy("customer_id", "customer_name", "city").agg(sum("total_amount").alias("total_spend"))

final_df = total_spend.join(order_count, "customer_id").withColumn(
        "segment",
        when(col("total_spend") > 10000, "Gold")
        .when((col("total_spend") >= 5000) & (col("total_spend") <= 10000), "Silver")
        .otherwise("Bronze")
    ).select("customer_name", "city", "total_spend", "order_count", "segment")

final_df.show()
