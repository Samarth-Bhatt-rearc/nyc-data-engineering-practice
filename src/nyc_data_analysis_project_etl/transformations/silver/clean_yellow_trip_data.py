from pyspark import pipelines as dp
from pyspark.sql import functions as F

PAYMENT_TYPE_LOOKUP = {
    "0": "Flex Fare Trip",
    "1": "Credit Card",
    "2": "Cash",
    "3": "No Charge",
    "4": "Dispute",
    "5": "Unknown",
    "6": "Voided Trip",
    "Cas": "Cash",
    "Csh": "Cash",
    "Cre": "Credit Card",
    "Na": "Unknown",
    "Dis": "Dispute",
    "No": "No Charge",
    "Noc": "No Charge",
    "Crd": "Credit Card",
    "Dispute": "Dispute",
    "Cash": "Cash",
    "Credit": "Credit Card",
    "No Charge": "No Charge",
}

VENDOR_LOOKUP = {
    "1": "Creative Mobile Technologies, LLC",
    "2": "Curb Mobility, LLC",
    "6": "Myle Technologies Inc",
    "7": "Helix",
    "Cmt": "Creative Mobile Technologies, LLC",
    "Vts": "VTS",
    "Dds": "DDS",
}

STORE_AND_FWD_FLAG_LOOKUP = {
    "Y": "Y",
    "N": "N",
    "0.0": "N",
    "1.0": "Y",
    "0": "N",
    "1": "Y",
}



# Trim whitespace from string columns only — trim() errors on non-string types
# (this table has timestamp/double/int columns alongside the string ones).
def trim_whitespace(df):
    string_cols = {f.name for f in df.schema.fields if f.dataType.typeName() == "string"}
    return df.select([F.trim(F.col(c)).alias(c) if c in string_cols else F.col(c) for c in df.columns])

# Map values with defaults
def mapping_with_defaults(df, lookup, column_name, default_value="Unknown", new_column_name=None):

    if not new_column_name:
        new_column_name = column_name

    # Initialize the expression with a dummy condition or the first key
    map_keys = list(lookup.keys())
    expr = F.when(F.col(new_column_name) == map_keys[0], F.lit(lookup[map_keys[0]]))

    # Dynamically chain the rest of the dictionary
    for key in map_keys[1:]:
        expr = expr.when(F.col(new_column_name) == key, F.lit(lookup[key]))

    # Provide a fallback default value to protect your 1.8B row dataset
    expr = expr.otherwise(F.lit(default_value))

    return df.withColumn(new_column_name, F.initcap(F.col(column_name))).withColumn(new_column_name, expr)

def replacing_nulls_with_default(df, column_name, default_value):
    return df.withColumn(column_name, F.when(F.col(column_name).isNull(), F.lit(default_value)).otherwise(F.col(column_name)))

@dp.table(
    name="yellow_tripdata_silver",
    comment=(
        "Yellow taxi trip records cleaned and transformed to a unified schema, incrementally ingested via Auto Loader."
    ),
)
def yellow_tripdata_silver():
    # .pipe() isn't supported on serverless (Spark Connect) — chain via plain calls instead.
    df = spark.readStream.table("yellow_tripdata_raw")
    df = trim_whitespace(df)
    df = mapping_with_defaults(df, PAYMENT_TYPE_LOOKUP, "payment_type")
    df = mapping_with_defaults(df, VENDOR_LOOKUP, "vendor_id", new_column_name="vendor_name")
    df = mapping_with_defaults(df, STORE_AND_FWD_FLAG_LOOKUP, "store_and_fwd_flag", "N")
    df = replacing_nulls_with_default(df, "passenger_count", 99)
    df = replacing_nulls_with_default(df, "rate_code_id", 99)
    return df