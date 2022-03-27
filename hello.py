from pyspark import SparkConf
from pyspark.sql import *

from lib.logger import Log4J
from lib.utils import get_spark_app_config

if __name__ == '__main__':

    '''spark = SparkSession \
        .builder \
        .appName("HelloSpark") \
        .master("local[3]") \
        .getOrCreate()

    conf = SparkConf()
    conf.set("spark.app.name", "hello Spark")
    conf.set("spark.master", "local[3]")'''

    conf = get_spark_app_config()
    spark = SparkSession \
        .builder \
        .config(conf=conf) \
        .getOrCreate()

    logger = Log4J(spark)

    logger.info("Starting HelloSpark")
    # processing code
    conf_out = spark.sparkContext.getConf()
    logger.info(conf_out.toDebugString())

    logger.info("Finished HelloSpark")
    #spark.stop()
