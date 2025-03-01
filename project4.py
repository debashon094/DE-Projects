from os import truncate

from pyspark.shell import spark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, DoubleType
from pyspark.sql.functions import col,lit,expr,sum,avg,max,min,mean,count

def classProject(spark):
    data = [ ("New York",10.0),
            ("New York",12.0),
            ("Los Angeles",20.0),
            ("Los Angeles",22.0),
             ("San Francisco",15.0),
              ("San Francisco",18.0)
    ]

    schema = StructType([
        StructField('city', StringType(),True),
        StructField('temperature', DoubleType(), True)
    ])

    df = spark.createDataFrame(data=data, schema=schema)
    df.show(truncate=False)

    df.groupby("city").agg(avg("temperature").alias("average_temperature"),\
        sum("temperature").alias("total_temperature"),\
        )\
        .show(truncate=False)

if __name__ == '__main__':
    classProject(spark)