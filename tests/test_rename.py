import time
import pytest
import pyspark
from deltawrapper._types import Column, Config
from delta import configure_spark_with_delta_pip
from deltawrapper.spark_utils import get_log4j_logger

import yaml

from deltawrapper._types import Column

# Add fixture for spark session
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
    logger = get_log4j_logger(spark, name=__name__)

    # Create a test dataframe
    output_cols = {"out-1": ["in-a", "in-b"], "out-2": ["in-c"], "out-3": ["in-d"]}
    output_df = [Column(name, aliases) for name, aliases in output_cols.items()]
    df_content = [
        (1, 2, 3, 4, 5),
        (5, 6, 7, 8, 5),
        (9, 10, 11, 12, 5),
    ]

    test_input = ["in-a", "in-b", "in-c", "in-d", "in-e"]

    df_input = spark.createDataFrame(
        df_content,
        test_input,
    )

    df_force = df_input

    logger.info("Testing rename_from_alias with force=True")
    for col in output_df:
        logger.info(f"Renaming column {col.name}")
        df_force = col.rename_from_alias(df_force, force=True)

    assert df_input.columns == ["in-a", "in-b", "in-c", "in-d", "in-e"]
    assert df_force.columns == ["out-1__in-a", "out-1__in-b", "out-2", "out-3", "in-e"]

    df_fail = df_input

    logger.info("Testing rename_from_alias with force=False")
    for col in output_df:
        logger.info(f"Renaming column {col.name}")
        if col == "out-1":
            with pytest.raises(ValueError):
                df_fail = col.rename_from_alias(df_fail, force=False)

    assert df_input.columns == ["in-a", "in-b", "in-c", "in-d", "in-e"]
    assert df_fail.columns == ["in-a", "in-b", "in-c", "in-d", "in-e"]
