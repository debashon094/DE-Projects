from pyspark.shell import spark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, DoubleType
from pyspark.sql.functions import col,lit

def classProject(spark):
    data = spark.sparkContext.textFile("file:///home/takeo/pycharmprojects/proj3.txt")

    schema = StructType([
        StructField('Name', StringType(),True),
        StructField('Age', StringType(), True),
        StructField('Height', StringType(), True)
    ])


    rdd = data.map(lambda x: x.split(','))
    df = spark.createDataFrame(data=rdd, schema=schema)
    df.show(truncate=False)

    df2  = df.distinct().show(truncate=False)

    df3 = df.dropDuplicates(["Age","Height"]).show(truncate=False)





if __name__ == '__main__':
    classProject(spark)