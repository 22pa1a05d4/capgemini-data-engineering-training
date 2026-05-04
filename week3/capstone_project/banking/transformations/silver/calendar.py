from pyspark.sql import functions as F
import dlt

@dlt.table(
    name="banking.silver.calendar",
    comment="Calendar - validated with weekend and holiday flags",
    table_properties={"quality": "silver", "layer": "silver"}
)
def calendar_silver():

    
    df = spark.table("banking.bronze.calendar_lookup")

    
    df = df.filter(F.col("Date").isNotNull())

    df = df.filter(F.col("Year") >= 2000)
    df = df.filter(F.col("Year") <= 2030)

    
    df = df.filter(F.col("Month") >= 1)
    df = df.filter(F.col("Month") <= 12)

   
    df = df.withColumn("Month_Name", F.trim(F.col("Month_Name")))
    df = df.withColumn("Day_Name",   F.trim(F.col("Day_Name")))
    df = df.withColumn("Quarter",    F.trim(F.col("Quarter")))


    # Spark: Sunday = 1, Saturday = 7
    df = df.withColumn("Is_Weekend",
        F.when(F.col("Day_of_Week").isin(1, 7), 1).otherwise(0)
    )
    df = df.withColumn("Is_Weekday",
        F.when(F.col("Is_Weekend") == 0, 1).otherwise(0)
    )
    df = df.dropDuplicates(["Date"])

  
    df = df.withColumn("_silver_processed_at", F.current_timestamp())

    return df