from pyspark.sql import functions as F
import dlt

@dlt.table(
    name="banking.silver.atm_locations",
    comment="ATM Locations - validated and cleaned",
    table_properties={"quality": "silver", "layer": "silver"}
)
def atm_locations_silver():

    df = spark.table("banking.bronze.atm_locations")
    df = df.filter(F.col("LocationID").isNotNull())

    
    df = df.filter(F.col("City").isNotNull())
    df = df.filter(F.col("State").isNotNull())
    df = df.filter(F.col("No_of_ATMs") > 0)
    df = df.withColumn("LocationID",    F.trim(F.col("LocationID")))
    df = df.withColumn("Location_Name", F.trim(F.col("Location_Name")))
    df = df.withColumn("City",          F.trim(F.col("City")))
    df = df.withColumn("State",         F.trim(F.col("State")))
    df = df.withColumn("Country",       F.trim(F.col("Country")))

    df = df.withColumn("Country", F.upper(F.col("Country")))

   
    df = df.dropDuplicates(["LocationID"])

    df = df.withColumn("_silver_processed_at", F.current_timestamp())

    return df