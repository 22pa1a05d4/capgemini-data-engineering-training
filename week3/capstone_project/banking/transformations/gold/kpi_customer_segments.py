from pyspark.sql import functions as F
import dlt

@dlt.table(
    name="banking.gold.kpi_customer_segments",
    comment="KPI - Customer behavior by age group, gender, account type",
    table_properties={"quality": "gold", "layer": "gold"}
)
def kpi_customer_segments_gold():

  
    df = spark.table("banking.gold.transactions_enriched")

    result = df.groupBy(
        "Age_Group", "Gender", "AccountType", "Occupation"
    ).agg(

        # Volume
        F.count("TransactionID")
         .alias("Total_Transactions"),

        F.countDistinct("CardholderID")
         .alias("Unique_Customers"),

        # Amount
        F.round(F.sum("TransactionAmount"), 2)
         .alias("Total_Amount"),

        F.round(F.avg("TransactionAmount"), 2)
         .alias("Avg_Amount_Per_Transaction"),

        # Transaction type breakdown
        F.count(F.when(F.col("TransactionTypeName") == "Withdrawal", 1))
         .alias("Total_Withdrawals"),

        F.count(F.when(F.col("TransactionTypeName") == "Deposit", 1))
         .alias("Total_Deposits"),

        F.count(F.when(F.col("TransactionTypeName") == "Transfer", 1))
         .alias("Total_Transfers"),

        # Avg transactions per customer
        F.round(
            F.count("TransactionID") / F.countDistinct("CardholderID"), 2
        ).alias("Avg_Transactions_Per_Customer")

    ).orderBy(F.desc("Total_Transactions"))

    result = result.withColumn("_gold_processed_at", F.current_timestamp())
    return result