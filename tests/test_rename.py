import pytest
import pyspark
from deltawrapper.types import Column

import logging

logging.basicConfig(level=logging.INFO)

# Add fixture for spark session
@pytest.fixture(scope="session")
def spark() -> None:
    import pyspark


from delta import *


@pytest.fixture(scope="session")
def spark() -> pyspark.sql.SparkSession:
    builder = (
        pyspark.sql.SparkSession.builder.appName("MyApp")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )

    return configure_spark_with_delta_pip(builder).getOrCreate()


def test_rename_column(spark: pyspark.sql.SparkSession) -> None:

    # Create a test dataframe
    output_cols = {"out-1": ["in-a", "in-b"], "out-2": ["in-c"], "out-3": ["in-d"]}
    output_df = [Column(name, aliases) for name, aliases in output_cols.items()]

    df = spark.createDataFrame(
        [
            (1, 2, 3, 4),
            (5, 6, 7, 8),
            (9, 10, 11, 12),
        ],
        ["in-a", "in-b", "in-c", "in-d"],
    )

    df_force = df

    logging.info("Testing rename_from_alias with force=True")
    for col in output_df:
        logging.info(f"Renaming column {col.name}")
        df_force = col.rename_from_alias(df_force, force=True)

    assert df.columns == ["in-a", "in-b", "in-c", "in-d"]
    assert df_force.columns == ["out-1__in-a", "out-1__in-b", "out-2", "out-3"]

    df_fail = df

    logging.info("Testing rename_from_alias with force=False")
    for col in output_df:
        logging.info(f"Renaming column {col.name}")
        if col == "out-1":
            with pytest.raises(ValueError):
                df_fail = col.rename_from_alias(df_fail, force=False)

    assert df.columns == ["in-a", "in-b", "in-c", "in-d"]
    assert df_fail.columns == ["in-a", "in-b", "in-c", "in-d"]
