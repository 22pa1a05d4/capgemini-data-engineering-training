
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, percent_rank
from pyspark.sql.window import Window
from pyspark.ml.feature import Bucketizer

# Create Spark Session
spark = SparkSession.builder.appName("Phase4A_Bucketing").getOrCreate()

# Load Data
df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("/samples/sales.csv")

df = df.withColumn("total_spend", col("total_amount"))
print("Bucketizer method")
splits = [-float("inf"), 5000, 10000, float("inf")]

bucketizer = Bucketizer(
    splits=splits,
    inputCol="total_spend",
    outputCol="bucket"
)

df = bucketizer.transform(df)

# -------------------------------
# 4. Window-based Ranking
# -------------------------------
window = Window.orderBy("total_spend")

df = df.withColumn(
    "rank_pct",
    percent_rank().over(window)
)

print("1.Create Gold/Silver/Bronze segmentation using conditional logic")
df = df.withColumn(
    "segment",
    when(col("total_spend") > 10000, "Gold")
    .when((col("total_spend") >= 5000) & (col("total_spend") <= 10000), "Silver")
    .otherwise("Bronze")
)
df.select("total_spend","segment").show()
print("2. Group data by segment and count customers")
df.groupBy("segment").count().show()
print("3.quantile-based segmentation")
quantiles = df.approxQuantile("total_spend", [0.33, 0.66], 0)

q1 = quantiles[0]
q2 = quantiles[1]

df = df.withColumn(
    "quantile_segment",
    when(col("total_spend") <= q1, "Bronze")
    .when((col("total_spend") > q1) & (col("total_spend") <= q2), "Silver")
    .otherwise("Gold")
)
df.select("total_spend", "quantile_segment").show(10)
print("4. Compare results of different methods")
df.select(
    "total_spend",
    "segment",
    "quantile_segment",
    "bucket",
    "rank_pct"
).show(20)


spark.stop()
