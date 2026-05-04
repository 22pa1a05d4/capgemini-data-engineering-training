from pyspark.sql import functions as F
import dlt

@dlt.table(
    name="banking.silver.transaction_types",
    comment="Transaction types - validated and cleaned",
    table_properties={"quality": "silver", "layer": "silver"}
)
def transaction_types_silver():

   
    df = spark.table("banking.bronze.transaction_type_lookup")

    df = df.filter(F.col("TransactionTypeID").isNotNull())
    df = df.filter(F.col("TransactionTypeName").isNotNull())

    df = df.withColumn(
        "TransactionTypeName",
        F.trim(F.col("TransactionTypeName"))
    )

    df = df.withColumn(
        "TransactionTypeName",
        F.initcap(F.col("TransactionTypeName"))
    )

   
    df = df.dropDuplicates(["TransactionTypeID"])

    df = df.withColumn("_silver_processed_at", F.current_timestamp())

    return df