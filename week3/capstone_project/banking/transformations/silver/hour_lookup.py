from pyspark.sql import functions as F
import dlt

@dlt.table(
    name="banking.silver.hour_lookup",
    comment="Hour lookup - validated with time of day segments",
    table_properties={"quality": "silver", "layer": "silver"}
)
def hour_lookup_silver():

   
    df = spark.table("banking.bronze.hour_lookup")

   
    df = df.filter(F.col("Hour_Key").isNotNull())
    df = df.filter(F.col("Hour_Key") >= 0)
    df = df.filter(F.col("Hour_Key") <= 23)

    df = df.withColumn("Time_Of_Day",
        F.when(F.col("Hour_Key").between(5,  11), "Morning")
         .when(F.col("Hour_Key").between(12, 16), "Afternoon")
         .when(F.col("Hour_Key").between(17, 20), "Evening")
         .otherwise("Night")
    )


    df = df.withColumn("Is_Peak_Hour",
        F.when(F.col("Hour_Key").between(9, 17), 1).otherwise(0)
    )

    df = df.dropDuplicates(["Hour_Key"])

    df = df.withColumn("_silver_processed_at", F.current_timestamp())

    return df