from pyspark.sql import functions as F
import dlt

@dlt.table(
    name="banking.gold.kpi_atm_performance",
    comment="KPI - ATM location performance and busiest ATMs",
    table_properties={"quality": "gold", "layer": "gold"}
)
def kpi_atm_performance_gold():

    df = spark.table("banking.gold.transactions_enriched")

    result = df.groupBy(
        "LocationID", "Location_Name", "City", "State",
        "Region", "No_of_ATMs"
    ).agg(
        F.count("TransactionID")
         .alias("Total_Transactions"),

        F.countDistinct("CardholderID")
         .alias("Unique_Customers"),

        F.round(F.sum("TransactionAmount"), 2)
         .alias("Total_Amount"),

        F.round(F.avg("TransactionAmount"), 2)
         .alias("Avg_Amount"),

        # Transactions per ATM machine (efficiency metric)
        F.round(
            F.count("TransactionID") / F.col("No_of_ATMs"), 2
        ).alias("Transactions_Per_ATM"),

        # Peak hour for this ATM
        F.round(F.avg("TransactionHour"), 0)
         .alias("Avg_Peak_Hour"),

        # Type breakdown
        F.count(F.when(F.col("TransactionTypeName") == "Withdrawal", 1))
         .alias("Withdrawals"),

        F.count(F.when(F.col("TransactionTypeName") == "Deposit", 1))
         .alias("Deposits"),

        F.round(F.avg("TransactionDurationMins"), 2)
         .alias("Avg_Duration_Mins")

    ).orderBy(F.desc("Total_Transactions"))

    result = result.withColumn("_gold_processed_at", F.current_timestamp())
    return result