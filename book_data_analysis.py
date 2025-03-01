from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Initialize Spark Session
spark = SparkSession.builder.appName("BookDataAnalysis").enableHiveSupport().getOrCreate()

df = spark.read.json('file:///home/takeo/pycharmprojects/books.json')
print(df.count())

print(df.distinct().count())

df = df.dropDuplicates()

df = df.select("title", when('ODD HOURS' != df.title, 1).otherwise(0).alias("newHours"))

df.withColumn("universal", col("title").like("%THE%")).show()

df.select("title").alias("universal").show()
