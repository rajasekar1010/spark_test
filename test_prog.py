from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from typing import List

spark = SparkSession.builder.appName("spark-unit-test").getOrCreate()


def to_uppercase(df: DataFrame, columns_to_transform: List) -> DataFrame:
    """Uppercase the columns provided in the dataframe
    Args:
        df (DataFrame): Input Dataframe
        columns_to_transform (List): List of columns to uppercase
    Returns:
        DataFrame: The transformed DataFrame
    """

    # Loop through columns to transform and convert to uppercase
    for column in columns_to_transform:
        if column in df.columns:
            df = df.withColumn(column, F.upper(F.col(column)))

    return df


if __name__ == "__main__":
    # Read the data
    df = spark.read.csv("C:\\Users\\raajs\\PycharmProjects\\pysparkproject\\data\\testing_data\\cars.csv", header=True)

    # Apply transformations
    df = to_uppercase(df, ["Brand", "Model"])

    # Write the data
    df.write.csv("C:\\Users\\raajs\\PycharmProjects\\pysparkproject\\data\\testing_data\\output\\")