
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("practice").getOrCreate()
customers_data = [
    (1, "Ravi", "Hyderabad", 25),
    (2, "Sita", "Chennai", 32),
    (3, "Arun", "Hyderabad", 28),
    (4, "Meena", "Bengaluru", 35),
    (5, "Kiran", "Chennai", 22)
]
orders_data = [
    (101, 1, 2500, "2026-03-01"),
    (102, 2, 1800, "2026-03-02"),
    (103, 1, 3200, "2026-03-03"),
    (104, 3, 1500, "2026-03-04"),
    (105, 5, 2800, "2026-03-05")
]
customers = spark.createDataFrame(customers_data, ["customer_id","customer_name","city","age"])
orders = spark.createDataFrame(orders_data, ["order_id","customer_id","amount","order_date"])
customers.join(orders,on="customer_id",how="inner").show()

print("1. Total amount for each customer")
orders.groupBy("customer_id").sum("amount").show()

print("2. Top 3 customers total amount spend")
orders.groupBy("customer_id").sum("amount").withColumnRenamed("sum(amount)","total_amount").orderBy("total_amount",ascending=False).limit(3).show()

print("3. Customers with no orders")
customers.join(orders,on="customer_id",how="left").filter(orders.order_id.isNull()).select(customers.customer_id,customers.customer_name,customers.city,customers.age).show()

print("4. Total cost by revenue")
customers.join(orders,on="customer_id",how="inner").groupBy("city").sum("amount").withColumnRenamed("sum(amount)","total_revenue").select("city","total_revenue").show()

print("5. Average Order amount per customer")
customers.join(orders,on="customer_id",how="inner").groupBy("customer_id","customer_name").avg("amount").withColumnRenamed("avg(amount)","avg_amount_per_customer").select("customer_name","avg_amount_per_customer").show()

print("6. Customer with more than one order")
customers.join(orders,on="customer_id",how="inner").groupBy("customer_id","customer_name").count().withColumnRenamed("count","Orders").filter("Orders>1").select("customer_name","Orders").show()

print("7. Sort customers by total spend in Descending order")
orders.groupBy("customer_id").sum("amount").withColumnRenamed("sum(amount)", "total_amount").orderBy("total_amount", ascending=False).show()
