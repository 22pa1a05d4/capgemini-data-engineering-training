from pyspark.sql import functions as F
import dlt

@dlt.table(
    name="banking.gold.kpi_transaction_type_summary",
    comment="KPI - Withdrawal vs Deposit vs Transfer breakdown",
    table_properties={"quality": "gold", "layer": "gold"}
)
def kpi_transaction_type_summary_gold():

    df = spark.table("banking.gold.transactions_enriched")

    result = df.groupBy(
        "TransactionTypeName", "Region", "Month_Name", "Quarter", "Year"
    ).agg(

        F.count("TransactionID")
         .alias("Total_Transactions"),

        F.round(F.sum("TransactionAmount"), 2)
         .alias("Total_Amount"),

        F.round(F.avg("TransactionAmount"), 2)
         .alias("Avg_Amount"),

        F.countDistinct("CardholderID")
         .alias("Unique_Customers"),

        F.round(F.avg("TransactionDurationMins"), 2)
         .alias("Avg_Duration_Mins"),

        # Weekend vs Weekday for each type
        F.count(F.when(F.col("Is_Weekend") == 1, 1))
         .alias("Weekend_Count"),

        F.count(F.when(F.col("Is_Weekend") == 0, 1))
         .alias("Weekday_Count")

    ).orderBy("Year", "Quarter", F.desc("Total_Amount"))

    result = result.withColumn("_gold_processed_at", F.current_timestamp())
    return result