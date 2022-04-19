from pyspark.sql import SparkSession, DataFrame

import pytest
from test_prog import *


# Get one spark session for the whole test session
@pytest.fixture(scope="session")
def spark_session():
    return SparkSession.builder.getOrCreate()


def test_uppercase(spark_session) -> None:

    # Set up test data
    columns = ["Brand", "Model", "Age"]
    data = [["Ford", "Focus", 12.5], ["Volkswagen", "Golf", 25.4]]

    # Set up expected data
    expected_data = [["FORD", "FOCUS", 12.5], ["VOLKSWAGEN", "GOLF", 25.4]]

    # Create dataframes
    df: DataFrame = spark_session.createDataFrame(data, columns)
    expected_df: DataFrame = spark_session.createDataFrame(expected_data, columns)

    # Apply Transformations
    upper_df = to_uppercase(df, ["Brand", "Model"])

    # Gather result rows
    rows = upper_df.collect()
    expected_rows = expected_df.collect()

    # Compare dataframes row by row
    for row_num, row in enumerate(rows):
        assert row == expected_rows[row_num]