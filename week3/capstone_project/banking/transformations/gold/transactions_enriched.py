from pyspark.sql import functions as F
import dlt

@dlt.table(
    name="banking.gold.transactions_enriched",
    comment="Master enriched table - transactions joined with all dimensions",
    table_properties={"quality": "gold", "layer": "gold"}
)
def transactions_enriched_gold():

   
    txn       = spark.table("banking.silver.transactions")
    customers = spark.table("banking.silver.customers")
    atm       = spark.table("banking.silver.atm_locations")
    calendar  = spark.table("banking.silver.calendar")
    hour      = spark.table("banking.silver.hour_lookup")
    txn_types = spark.table("banking.silver.transaction_types")

    # ── STEP 2: Join transactions with customers ──────────────────────
    df = txn.join(
        customers.select(
            "CardholderID", "Full_Name", "Gender",
            "Age", "Age_Group", "AccountType", "Occupation"
        ),
        on="CardholderID",
        how="left"
    )

    # ── STEP 3: Join with ATM locations ──────────────────────────────
    df = df.join(
        atm.select(
            "LocationID", "Location_Name",
            "City", "State", "No_of_ATMs"
        ),
        on="LocationID",
        how="left"
    )

    # ── STEP 4: Join with calendar ────────────────────────────────────
    df = df.join(
        calendar.select(
            "Date", "Month_Name", "Month", "Quarter",
            "Year", "Day_Name", "Is_Weekend",
            "Is_Weekday", "IsHoliday"
        ),
        df["TransactionDate"] == calendar["Date"],
        how="left"
    ).drop("Date")

    df = df.join(
        hour.select("Hour_Key", "Time_Of_Day", "Is_Peak_Hour"),
        df["TransactionHour"] == hour["Hour_Key"],
        how="left"
    ).drop("Hour_Key")

   
    df = df.join(
        txn_types.select("TransactionTypeID", "TransactionTypeName"),
        on="TransactionTypeID",
        how="left"
    )

    df = df.withColumn("_gold_processed_at", F.current_timestamp())

    return df