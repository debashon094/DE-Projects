from os import truncate
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import explode,split, array_contains, array
from pyspark.shell import spark

def pysparkPartition(spark):
    df = spark.read.option("header", True) \
        .csv("file:///home/takeo/simple-zipcodes.csv")
    df.printSchema()

def partitionby(spark):
    # df = spark.read.option("header", True) \
    #     .csv("file:///home/takeo/simple-zipcodes.csv")
    # df.write.option("header", True) \
    #     .partitionBy("state") \
    #     .mode("overwrite") \
    #     .csv("file:///tmp/parts/zipcodes-state")

    # #df.write.option("header", True) \
    #     .partitionBy("state", "city") \
    #     .mode("overwrite") \
    #     .csv("file:///tmp/parts/zipcodes-city-state")

    # df.repartition(2).write.option("header", True).partitionBy("state").mode("overwrite").csv(
    #     "file:///tmp/parts/zipcodes-state-more")
    #
    # df.write.option("header",True).option("maxRecordsPerFile",2).partitionBy("state").mode("overwrite").csv("file:///tmp/parts/multi-zipcodes-state")
    # df.show()
    #
    # dfSinglePart = spark.read.option("header",True).csv("file:////tmp/parts/zipcodes-city-state/state=AL/city=SPRINGVILLE")
    # dfSinglePart.printSchema()
    # dfSinglePart.show()

    parqDF = spark.read.option("header", True) \
        .csv("file:////tmp/parts/zipcodes-city-state/")
    parqDF.createOrReplaceTempView("ZIPCODE")
    spark.sql("select * from ZIPCODE  where state='AL' and city = 'SPRINGVILLE'") \
        .show()

def arrayType(spark):
    data = [
        ("James,,Smith", ["Java", "Scala", "C++"], ["Spark", "Java"], "OH", "CA"),
        ("Michael,Rose,", ["Spark", "Java", "C++"], ["Spark", "Java"], "NY", "NJ"),
        ("Robert,,Williams", ["CSharp", "VB"], ["Spark", "Python"], "UT", "NV")
    ]
    schema = StructType([
        StructField("name", StringType(), True),
        StructField("languagesAtSchool", ArrayType(StringType()), True),
        StructField("languagesAtWork", ArrayType(StringType()), True),
        StructField("currentState", StringType(), True),
        StructField("previousState", StringType(), True)
    ])

    df = spark.createDataFrame(data=data, schema=schema)
    df.printSchema()
    df.show()

    df.select(df.name, explode(df.languagesAtSchool)).show()

    df.select(split(df.name, ",").alias("nameAsArray")).show()

    df.select(df.name, array(df.currentState, df.previousState).alias("States")).show()

    df.select(df.name, array_contains(df.languagesAtSchool, "Java").alias("array_contains")).show()
def mapType(spark):
    schema = StructType([
        StructField('name', StringType(), True),
        StructField('properties', MapType(StringType(), StringType()), True)
    ])
    dataDictionary = [
        ('James', {'hair': 'black', 'eye': 'brown'}),
        ('Michael', {'hair': 'brown', 'eye': None}),
        ('Robert', {'hair': 'red', 'eye': 'black'}),
        ('Washington', {'hair': 'grey', 'eye': 'grey'}),
        ('Jefferson', {'hair': 'brown', 'eye': ''})
    ]
    df = spark.createDataFrame(data=dataDictionary, schema=schema)
    df.printSchema()
    df.show(truncate=False)

    df3 = df.rdd.map(lambda x: \
                         (x.name, x.properties["hair"], x.properties["eye"])) \
        .toDF(["name", "hair", "eye"])
    df3.printSchema()
    df3.show()

    df.withColumn("hair",df.properties.getItem("hair")) \
      .withColumn("eye",df.properties.getItem("eye")) \
      .drop("properties") \
      .show()


if __name__ == '__main__':
    #pysparkPartition(spark)
    #partitionby(spark)
    #arrayType(spark)
    mapType(spark)
