from os import truncate
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.shell import spark


def spark_project(spark):
    product_schema = StructType() \
        .add("product_id", IntegerType(), True)\
        .add("product_name",StringType(),True)\
        .add("unit_Price",IntegerType(),True)

    product_df = spark.read.options(inferSchema='True', delimiter=',').schema(product_schema).csv("file:///home/takeo/pycharmprojects/product_data.csv")
    product_df.printSchema()
    product_df.show()

    product_df.write.mode("overwrite").format("parquet").saveAsTable("xyz.product_Table")
    non_partition_table = spark.sql("select * from xyz.product_Table")
    non_partition_table.show(truncate=False)

    sales_schema = StructType()\
              .add("seller_id",IntegerType(),True)\
              .add("product_id", IntegerType(), True)\
              .add("buyer_id",IntegerType(),True)\
              .add("sale_date", StringType(), True)\
              .add("quantity", IntegerType(), True)\
              .add("price", IntegerType(), True)

    sales_df = spark.read.options(inferSchema='True', delimiter=',').schema(sales_schema).csv(
        "file:///home/takeo/pycharmprojects/sales_data.csv")
    sales_df.printSchema()
    sales_df.show(truncate=False)
    spark.conf.set("hive.exec.dynamic.partition", "true")
    spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
    sales_df.write.partitionBy("sale_date").mode("overwrite").format("parquet").saveAsTable("xyz.sales_Table")
    partition_table = spark.sql("select * from xyz.sales_Table")
    partition_table.show(truncate=False)

    sdf = spark.sql("SELECT DISTINCT o.buyer_id FROM xyz.sales_Table o JOIN xyz.product_Table p1 ON o.product_id = p1.product_id WHERE p1.product_name = 'S8' AND o.buyer_id NOT IN \
    (SELECT DISTINCT o2.buyer_id FROM xyz.sales_Table o2 JOIN xyz.product_Table p2 ON o2.product_id = p2.product_id WHERE p2.product_name = 'iPhone'')'")
    sdf.printSchema()
    sdf.show(truncate=False)


if __name__ == '__main__':
  spark_project(spark)
