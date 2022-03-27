from pyspark.sql import *
from pyspark.sql import functions as f
from pyspark.sql.functions import weekofyear

from lib.logger import Log4J
from lib.utils import get_spark_app_config

if __name__ == "__main__":
    conf = get_spark_app_config()
    spark = SparkSession.builder \
        .config(conf=conf) \
        .getOrCreate()

    logger = Log4J(spark)

    logger.info("Starting HelloSpark")
    invoice_df = spark.read.format('csv').option("inferschema", True) \
        .option("header", True) \
        .load(
        "C:\\Users\\raajs\\Google Drive\\My Files\\Big Data\\Udemy Cognizant\\Spark-Programming-In-Python-master\\13-AggDemo\\data\\invoices.csv")

    # To extract date from a string pattern
    #       '01-12-2010 8.26'
    from_pattern='dd-MM-yyyy h.mm'
    to_pattern = 'dd-MM-yyyy'
    df1=invoice_df.withColumn('date', f.from_unixtime(f.unix_timestamp(invoice_df['InvoiceDate'], from_pattern), to_pattern))
    df1.show()
    df1.printSchema()

    # aggregate logic to find the below - using dataframe
    NumInvoices = f.countDistinct("InvoiceNo").alias('Number_of_invoices')
    SumQuantity = f.sum("Quantity").alias('Total_Quantity')
    InvoiceValue = f.round(f.sum(f.expr("Quantity * UnitPrice")), 2).alias('invoice_value')

    invoice_df.withColumn('InvoiceDate', f.to_date(f.col("InvoiceDate"), "dd-MM-yyyy H.mm"))\
        .withColumn('WeekNumber', f.weekofyear('InvoiceDate'))\
        .where("year(InvoiceDate) == 2010")\
        .groupBy('country', "WeekNumber") \
        .agg(NumInvoices, SumQuantity, InvoiceValue)\
        .sort('Country', "WeekNumber")\
        .show(invoice_df.count(), False)

    # writing to location
    exSummary_df = invoice_df.withColumn('InvoiceDate', f.to_date(f.col("InvoiceDate"), "dd-MM-yyyy H.mm"))\
        .withColumn('WeekNumber', f.weekofyear('InvoiceDate'))\
        .where("year(InvoiceDate) == 2010")\
        .groupBy('country', "WeekNumber") \
        .agg(NumInvoices, SumQuantity, InvoiceValue)\
        .sort('Country', "WeekNumber")

    exSummary_df.coalesce(1)\
        .write\
        .format('parquet')\
        .mode("overwrite")\
        .save('output')

    # aggregate function using - dataframe expression

    summary_df = invoice_df \
        .groupBy("Country", "InvoiceNo") \
        .agg(f.sum("Quantity").alias("TotalQuantity"),
             f.round(f.sum(f.expr("Quantity * UnitPrice")), 2).alias("InvoiceValue"),
             f.expr("round(sum(Quantity * UnitPrice),2) as InvoiceValueExpr")
             )

    summary_df.show()

    # aggregate function - using expression

    invoice_df.selectExpr(
        "count(1) as `count 1`",
        "count(StockCode) as `count field`",
        "sum(Quantity) as TotalQuantity",
        "avg(UnitPrice) as AvgPrice"
    ).show()

    # aggregate using - SQL query

    invoice_df.createOrReplaceTempView("sales")
    summary_sql = spark.sql("""
              SELECT Country, InvoiceNo,
                    sum(Quantity) as TotalQuantity,
                    round(sum(Quantity*UnitPrice),2) as InvoiceValue
              FROM sales
              GROUP BY Country, InvoiceNo""")

    summary_sql.show()

    # aggregate function using - windowing function

    summary_df = spark.read.parquet('C:\\Users\\raajs\\PycharmProjects\\pysparkproject\\output')
    summary_df.show()
    window = Window.partitionBy("country").orderBy('Country', 'WeekNumber') \
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)

    summary_df \
        .withColumn('Running Total', f.sum('invoice_value').over(window)) \
        .show()
