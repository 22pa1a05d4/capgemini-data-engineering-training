from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("Phase3").getOrCreate()
sales=spark.read.format("csv")\
.option("header","true")\
.load("/samples/sales.csv")

customers=spark.read.format("csv")\
.option("header","true")\
.load("/samples/customers.csv")

products=spark.read.format("csv")\
.option("header","true")\
.load("/samples/products.csv")

sales.show()
sales.printSchema()
sales=sales.dropna()

customers.show()
customers.printSchema()
customers=customers.dropna()


sales=sales.withColumn("total_amount",col("total_amount").cast("int"))

print("1. Calculating daily sales")
sales.groupBy("sale_date").sum("total_amount").withColumnRenamed("sum(total_amount)","Daily_sales").select("sale_date","Daily_sales").show()

print("2. City wise Revenue")
sales.join(customers,on="customer_id",how="inner").groupBy("city").sum("total_amount").withColumnRenamed("sum(total_amount)","total_revenue").select("city","total_revenue").show()

print("3. Repeat customers (>2 orders)")
sales.groupBy("customer_id").count().filter("count > 2").withColumnRenamed("count", "order_count").show()

print("4. Find highest spending customer in each city")
# step 1: Join Sales and Customers Data
data = sales.join(customers, "customer_id")

# Step 2: Total spend per customer per city
customer_spend = data.groupBy("city", "customer_id", "first_name") \
    .sum("total_amount")

# Step 3: Max spend per city
max_spend = customer_spend.groupBy("city") \
    .max("sum(total_amount)")

# Step 4: Use aliases to avoid ambiguity
cs = customer_spend.alias("cs")
ms = max_spend.alias("ms")

result = cs.join(
    ms,
    (col("cs.city") == col("ms.city")) &
    (col("cs.sum(total_amount)") == col("ms.max(sum(total_amount))"))
).select(
    col("cs.city"),
    col("cs.customer_id"),
    col("cs.first_name"),
    col("cs.sum(total_amount)").alias("total_spend")
)

result.show()

print("5. Final reporting table")
sales.join(customers,on="customer_id",how="inner") \
.groupBy("customer_id","first_name","city") \
.agg({"total_amount":"sum","sale_id":"count"}) \
.withColumnRenamed("sum(total_amount)","total_spend") \
.withColumnRenamed("count(sale_id)","order_count") \
.select("customer_id","first_name","city","total_spend","order_count") \
.show()
