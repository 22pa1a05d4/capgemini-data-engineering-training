from pyspark.sql import functions as F
import dlt

@dlt.table(
    name="banking.gold.kpi_regional_summary",
    comment="KPI - Transaction summary by Nigerian region",
    table_properties={"quality": "gold", "layer": "gold"}
)
def kpi_regional_summary_gold():

    df = spark.table("banking.gold.transactions_enriched")

    result = df.groupBy("Region", "State", "City").agg(

        # Volume KPIs
        F.count("TransactionID")
         .alias("Total_Transactions"),

        F.countDistinct("CardholderID")
         .alias("Unique_Customers"),

        F.countDistinct("LocationID")
         .alias("Unique_ATMs_Used"),

        # Amount KPIs
        F.round(F.sum("TransactionAmount"), 2)
         .alias("Total_Transaction_Amount"),

        F.round(F.avg("TransactionAmount"), 2)
         .alias("Avg_Transaction_Amount"),

        F.round(F.max("TransactionAmount"), 2)
         .alias("Max_Transaction_Amount"),

        F.round(F.min("TransactionAmount"), 2)
         .alias("Min_Transaction_Amount"),

        # Duration KPI
        F.round(F.avg("TransactionDurationMins"), 2)
         .alias("Avg_Duration_Mins"),

        # Weekend vs Weekday split
        F.count(F.when(F.col("Is_Weekend") == 1, 1))
         .alias("Weekend_Transactions"),

        F.count(F.when(F.col("Is_Weekend") == 0, 1))
         .alias("Weekday_Transactions"),

        # Holiday transactions
        F.count(F.when(F.col("IsHoliday") == 1, 1))
         .alias("Holiday_Transactions")

    ).orderBy(F.desc("Total_Transaction_Amount"))

    result = result.withColumn("_gold_processed_at", F.current_timestamp())
    return result