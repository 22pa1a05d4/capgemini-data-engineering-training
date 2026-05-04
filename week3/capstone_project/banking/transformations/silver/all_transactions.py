from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
import dlt

@dlt.table(
    name="banking.silver.transactions",
    comment="All regional transactions unioned, validated and cleaned",
    table_properties={"quality": "silver", "layer": "silver"}
)
def transactions_silver():

  
    enugu  = spark.table("banking.bronze.enugu_transactions")
    fct    = spark.table("banking.bronze.fct_transactions")
    kano   = spark.table("banking.bronze.kano_transactions")
    lagos  = spark.table("banking.bronze.lagos_transactions")

   
    cols = [
        "TransactionID",
        "TransactionStartDateTime",
        "TransactionEndDateTime",
        "CardholderID",
        "LocationID",
        "TransactionTypeID",
        "TransactionAmount",
        "_source_file"
    ]
    enugu  = enugu.select(cols)
    fct    = fct.select(cols)
    kano   = kano.select(cols)
    lagos  = lagos.select(cols)

    # Union all 4 regional tables into one 
    df = enugu.union(fct).union(kano).union(lagos)

    df = df.filter(F.col("TransactionID").isNotNull())
    df = df.filter(F.col("CardholderID").isNotNull())
    df = df.filter(F.col("LocationID").isNotNull())
    df = df.filter(F.col("TransactionTypeID").isNotNull())
    df = df.filter(F.col("TransactionAmount").isNotNull())
    df = df.filter(F.col("TransactionAmount") > 0)
    df = df.dropDuplicates(["TransactionID"])

    df = df.withColumn(
        "TransactionStartDateTime",
        F.to_timestamp("TransactionStartDateTime", "M/d/yyyy H:mm")
    )
    df = df.withColumn(
        "TransactionEndDateTime",
        F.to_timestamp("TransactionEndDateTime", "M/d/yyyy H:mm")
    )
    df = df.filter(F.col("TransactionStartDateTime").isNotNull())
    df = df.filter(F.col("TransactionEndDateTime").isNotNull())

   
    df = df.withColumn(
        "TransactionAmount",
        F.col("TransactionAmount").cast(DoubleType())
    )


    df = df.withColumn(
        "TransactionDurationMins",
        F.round(
            (F.unix_timestamp("TransactionEndDateTime") -
             F.unix_timestamp("TransactionStartDateTime")) / 60, 2
        )
    )

    # Extract date → for joining with calendar in Gold
    df = df.withColumn(
        "TransactionDate",
        F.to_date("TransactionStartDateTime")
    )
    
    # Extract hour → for joining with hour_lookup in Gold
    df = df.withColumn(
        "TransactionHour",
        F.hour("TransactionStartDateTime")
    )


    df = df.withColumn(
        "Region",
        F.when(F.col("_source_file").contains("lagos"),  "Lagos")
         .when(F.col("_source_file").contains("kano"),   "Kano")
         .when(F.col("_source_file").contains("enugu"),  "Enugu")
         .when(F.col("_source_file").contains("rivers"), "Rivers")
         .when(F.col("_source_file").contains("fct"),    "FCT - Abuja")
         .otherwise("Unknown")
    )

    df = df.withColumn("_silver_processed_at", F.current_timestamp())
     
    return df
