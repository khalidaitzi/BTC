from snowflake.snowpark.functions import col, parse_json


def model(dbt, session):

    dbt.config(
        materialized="table"
    )

    btc = dbt.ref("stg_btc")

    # OUTPUTS is expected to contain a JSON array stored as VARCHAR.
    btc_parsed = btc.with_column(
        "OUTPUTS_PARSED",
        parse_json(col("OUTPUTS"))
    )

    # Correct syntax: pass "flatten" directly as the function name.
    btc_exploded = btc_parsed.join_table_function(
        "flatten",
        col("OUTPUTS_PARSED")
    )

    # Keep the original columns and temporarily rename FLATTEN.VALUE
    # to prevent confusion with the final VALUE column.
    output_items = btc_exploded.select(
        *[
            col(column_name)
            for column_name in btc.columns
            if column_name.upper() != "OUTPUTS"
        ],
        col("VALUE").alias("OUTPUT_ITEM")
    )

    result = (
        output_items
        .with_column(
            "ADDRESS",
            col("OUTPUT_ITEM")["address"].cast("string")
        )
        .with_column(
            "VALUE",
            col("OUTPUT_ITEM")["value"]
        )
        .filter(
            col("ADDRESS").is_not_null()
        )
        .drop("OUTPUT_ITEM")
    )

    return result