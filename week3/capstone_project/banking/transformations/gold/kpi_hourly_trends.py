from pyspark.sql import functions as F
import dlt

@dlt.table(
    name="banking.gold.kpi_hourly_trends",
    comment="KPI - Peak ATM usage hours and time of day trends",
    table_properties={"quality": "gold", "layer": "gold"}
)
def kpi_hourly_trends_gold():

    df = spark.table("banking.gold.transactions_enriched")

    result = df.groupBy(
        "TransactionHour", "Time_Of_Day", "Is_Peak_Hour"
    ).agg(

        F.count("TransactionID")
         .alias("Total_Transactions"),

        F.round(F.sum("TransactionAmount"), 2)
         .alias("Total_Amount"),

        F.round(F.avg("TransactionAmount"), 2)
         .alias("Avg_Amount"),

        F.countDistinct("CardholderID")
         .alias("Unique_Customers"),

        # Breakdown by transaction type
        F.count(F.when(F.col("TransactionTypeName") == "Withdrawal", 1))
         .alias("Withdrawals"),

        F.count(F.when(F.col("TransactionTypeName") == "Deposit", 1))
         .alias("Deposits"),

        F.count(F.when(F.col("TransactionTypeName") == "Transfer", 1))
         .alias("Transfers"),

        F.round(F.avg("TransactionDurationMins"), 2)
         .alias("Avg_Duration_Mins")

    ).orderBy("TransactionHour")

    result = result.withColumn("_gold_processed_at", F.current_timestamp())
    return result