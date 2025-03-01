from os import truncate
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark = SparkSession.builder.appName("SparkCovid19Analysis").enableHiveSupport().getOrCreate()

def hive_table_creation(spark):
    print("test")

if __name__ == '__main__':
    hive_table_creation(spark)


