from os import truncate
import pyspark
from pyspark.sql import SparkSession
from pyspark.shell import spark
from pyspark.sql.functions import row_number,rank,dense_rank,lag,lead
from pyspark.sql.window import Window
from pyspark import SparkContext

def sparkcasestudy(spark):
    sc = SparkContext("local", "E-Commerce Analysis")
    data = [(1, 101, 5001, 'Laptop', 'Electronics', 1000.0, 1),
                   (2, 102, 5002, 'Headphones', 'Electronics', 50.0, 2),
                   (3, 101, 5003, 'Book', 'Books', 20.0, 3),
                   (4, 103, 5004, 'Laptop', 'Electronics', 1000.0, 1),
                   (5, 102, 5005, 'Chair', 'Furniture', 150.0, 1)
                ]

    transactions_rdd = sc.parallelize(data)
    transactions_tuple_rdd = transactions_rdd.map(lambda line:line.split(","))
    high_quality_rdd = transactions_tuple_rdd.filter(lambda  x:x[[3]])

if __name__ == '__main__':
    sparkcasestudy(spark)