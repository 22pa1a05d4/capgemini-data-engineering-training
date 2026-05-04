from pyspark.sql import functions as F
import dlt

@dlt.table(
    name="banking.silver.customers",
    comment="Customers - validated, cleaned, age and segments added",
    table_properties={"quality": "silver", "layer": "silver"}
)
def customers_silver():

 
    df = spark.table("banking.bronze.customers_lookup")
    df = df.filter(F.col("CardholderID").isNotNull())
    df = df.filter(F.col("First_Name").isNotNull())
    df = df.filter(F.col("Gender").isNotNull())
    df = df.filter(F.col("Birth_Date").isNotNull())
    df = df.filter(F.col("AccountType").isNotNull())
    df = df.withColumn("CardholderID", F.trim(F.col("CardholderID")))
    df = df.withColumn("First_Name",   F.trim(F.col("First_Name")))
    df = df.withColumn("Last_Name",    F.trim(F.col("Last_Name")))
    df = df.withColumn("Gender",       F.trim(F.col("Gender")))
    df = df.withColumn("Occupation",   F.trim(F.col("Occupation")))
    df = df.withColumn("AccountType",  F.trim(F.col("AccountType")))

  
    df = df.withColumn("Gender",
        F.when(F.upper(F.col("Gender")).isin("M", "MALE"),   "Male")
         .when(F.upper(F.col("Gender")).isin("F", "FEMALE"), "Female")
         .otherwise("Unknown")
    )

    df = df.withColumn(
        "Full_Name",
        F.concat_ws(" ", F.col("First_Name"), F.col("Last_Name"))
    )


    df = df.withColumn(
        "Age",
        F.floor(F.datediff(F.current_date(), F.col("Birth_Date")) / 365)
    )

    df = df.filter(F.col("Age") >= 18)
    df = df.filter(F.col("Age") <= 100)

    df = df.withColumn("Age_Group",
        F.when(F.col("Age") < 25, "18-24")
         .when(F.col("Age") < 35, "25-34")
         .when(F.col("Age") < 45, "35-44")
         .when(F.col("Age") < 55, "45-54")
         .otherwise("55+")
    )

    df = df.dropDuplicates(["CardholderID"])

    df = df.withColumn("_silver_processed_at", F.current_timestamp())

    return df