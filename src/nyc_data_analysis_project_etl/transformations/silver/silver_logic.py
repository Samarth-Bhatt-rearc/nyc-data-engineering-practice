# Pure DataFrame logic for the silver layer, kept free of any `pyspark.pipelines`
# import so it can be unit tested directly — see tests/test_silver_logic.py.
from pyspark.sql import functions as F

# Digit codes (modern era) and legacy-era text codes both map to one canonical label.
# Matched against the column AFTER F.initcap(), so keys must already be in initcap
# form (e.g. "Csh" not "CSH") or they will silently never match.
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

# Same idea as PAYMENT_TYPE_LOOKUP: modern numeric vendor IDs plus legacy-era
# 3-letter codes (also matched post-initcap), mapped to a readable vendor name.
VENDOR_LOOKUP = {
    "1": "Creative Mobile Technologies, LLC",
    "2": "Curb Mobility, LLC",
    "6": "Myle Technologies Inc",
    "7": "Helix",
    "Cmt": "Creative Mobile Technologies, LLC",
    "Vts": "VTS",
    "Dds": "DDS",
}

# Bronze casts the 2009-era store_and_forward double straight to string, so this
# also has to cover "0.0"/"1.0"/"0"/"1" alongside the modern "Y"/"N" flag values.
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
    """Trim leading/trailing whitespace from every string column, leaving other columns untouched.

    Args:
        df: DataFrame to trim.
    """
    string_cols = {f.name for f in df.schema.fields if f.dataType.typeName() == "string"}
    return df.select([F.trim(F.col(c)).alias(c) if c in string_cols else F.col(c) for c in df.columns])


def mapping_with_defaults(df, lookup, column_name, default_value="Unknown", new_column_name=None):
    """Initcap a column and map it through a lookup dict, falling back to a default for unmatched values.

    Args:
        df: DataFrame containing the column to map.
        lookup: Dict of {initcapped_raw_value: cleaned_value} (e.g. PAYMENT_TYPE_LOOKUP).
        column_name: Name of the source column to read and initcap.
        default_value: Value to use when the initcapped value has no entry in lookup.
        new_column_name: Column to write the mapped result to; defaults to
            column_name (in place) when not given, otherwise column_name is
            left untouched and this new column is added alongside it.
    """
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
    """Replace null values in a column with a default, leaving non-null values unchanged.

    Args:
        df: DataFrame containing the column.
        column_name: Name of the column to fill nulls in.
        default_value: Value to substitute wherever column_name is null.
    """
    return df.withColumn(column_name, F.when(F.col(column_name).isNull(), F.lit(default_value)).otherwise(F.col(column_name)))
