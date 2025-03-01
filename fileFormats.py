from os import truncate
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.shell import spark


def fileformat(spark):
    schema = StructType() \
        .add("RecordNumber", IntegerType(), True) \
        .add("Zipcode", IntegerType(), True) \
        .add("ZipCodeType", StringType(), True) \
        .add("City", StringType(), True) \
        .add("State", StringType(), True) \
        .add("LocationType", StringType(), True) \
        .add("Lat", DoubleType(), True) \
        .add("Long", DoubleType(), True) \
        .add("Xaxis", IntegerType(), True) \
        .add("Yaxis", DoubleType(), True) \
        .add("Zaxis", DoubleType(), True) \
        .add("WorldRegion", StringType(), True) \
        .add("Country", StringType(), True) \
        .add("LocationText", StringType(), True) \
        .add("Location", StringType(), True) \
        .add("Decommisioned", BooleanType(), True) \
        .add("TaxReturnsFiled", StringType(), True) \
        .add("EstimatedPopulation", IntegerType(), True) \
        .add("TotalWages", IntegerType(), True) \
        .add("Notes", StringType(), True)

    #df = spark.read.options(header='True',inferSchema='True').schema(schema).csv("file:///home/takeo/zipcodes.csv")
    #df.printSchema()

    df_with_schema = spark.read.format("csv").option("header",True).schema(schema).load("file:///home/takeo/zipcodes.csv")
    df_with_schema.printSchema()


    #overwrite_Mode

    df_with_schema.write.format("csv").mode('overwrite').save("file:///tmp/spark_output/zipcodes")

def parqueteDF(spark):
    data = [("James ", "", "Smith", "36636", "M", 3000),
            ("Michael ", "Rose", "", "40288", "M", 4000),
            ("Robert ", "", "Williams", "42114", "M", 4000),
            ("Maria ", "Anne", "Jones", "39192", "F", 4000),
            ("Jen", "Mary", "Brown", "", "F", -1)]
    columns = ["firstname", "middlename", "lastname", "dob", "gender", "salary"]
    df = spark.createDataFrame(data,columns)
    df.write.mode("overwrite").parquet("file:///tmp/output/people.parquet")

    parDF = spark.read.parquet("file:///tmp/output/people.parquet")

    parDF.createOrReplaceTempView("ParquetTable")
    parkSQL = spark.sql("select * from ParquetTable where salary >= 4000")
    parkSQL.show()

if __name__ == '__main__':
    #fileformat(spark)
    parqueteDF(spark)