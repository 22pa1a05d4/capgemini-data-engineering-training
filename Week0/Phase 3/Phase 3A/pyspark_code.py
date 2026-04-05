from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("Phase3A").getOrCreate()

data = [
(1, "Ravi", "Hyderabad", 25),
(2, None, "Chennai", 32),
(None, "Arun", "Hyderabad", 28),
(4, "Meena", None, 30),
(4, "Meena", None, 30),
(5, "John", "Bangalore", -5)
]

columns = ["customer_id", "name", "city", "age"]

df = spark.createDataFrame(data, columns)

df.show()
df.printSchema()
print("1. Identify data issues (nulls, duplicates, invalid values)")
df.describe().show()
print("2.Clean data (remove null keys, handle missing values, remove duplicates, filter invalid age")
df_clean = df.filter(col("customer_id").isNotNull())
df_clean = df_clean.fillna({
    "name": "Unknown",
    "city": "Unknown"
})
df_clean = df_clean.dropDuplicates()
df_clean = df_clean.filter(col("age") > 0)
df_clean.show()
print("3. Validate cleaning (row counts before and after)")
print("Before cleaning:", df.count())
print("After cleaning:", df_clean.count())
df_clean.show()
print("4. Perform aggregation (customers per city)")
df_clean.groupBy("city").count().show()
