from pyspark.sql import functions as F
import dlt

SOURCE_PATH = "s3://bhavagna-capstone/data-store/banking-data"

def clean_column_names(df):
    new_columns = []
    for c in df.columns:
        clean = c
        for char in [' ', ',', ';', '{', '}', '(', ')', '\n', '\t', '=']:
            clean = clean.replace(char, '_')
        clean = clean.strip('_')
        new_columns.append(clean)
    return df.toDF(*new_columns)

@dlt.table(
    name="banking.bronze.fct_transactions",
    comment="Fact Transactions Raw Data",
    table_properties={
        "quality": "bronze",
        "layer": "bronze",
        "source_format": "csv"
    }
)
def fct_transactions_bronze():
    df = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .option("escape", '"') \
        .load(f"{SOURCE_PATH}/fct_transactions.csv")
    df = clean_column_names(df)
    df = df.withColumn("_ingested_at", F.current_timestamp()) \
           .withColumn("_source_file", F.lit("fct_transactions.csv"))
    return df