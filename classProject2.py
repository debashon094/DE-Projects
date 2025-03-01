from pyspark.shell import spark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, ArrayType
from pyspark.sql.functions import col,lit

def classColumnwith(spark):
    data = spark.sparkContext.textFile("file:///home/takeo/pycharmprojects/data.txt")
    columns = ["carr", "horsepower", "weight", "origin"]
    rdd = data.map(lambda x: x.split(','))
    df = spark.createDataFrame(data=rdd, schema=columns)
    df.show(truncate=False)
    df.withColumn("AvgWeight", lit("200")).show()
    ddf = df.withColumn("kilowatt_power", col("horsepower")*1000)
    ddf.show(truncate=False)
    ddf.withColumnRenamed("carr", "car").show(truncate=False)

if __name__=='__main__':
    classColumnwith(spark)