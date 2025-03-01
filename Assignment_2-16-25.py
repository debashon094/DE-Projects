from os import truncate
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.shell import spark
from pyspark.sql.types import StringType,IntegerType,StructType,StructField

def question1(spark):
    data = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
    rdd = spark.sparkContext.parallelize(data, 5)
    num_partitions = rdd.getNumPartitions()
    print(f"Number of partitions: {num_partitions}")

def question2(spark):
    rdd = spark.sparkContext.textFile("file:///home/takeo/pycharmprojects/question2.txt")
    rdd2 = rdd.count()
    print(f"Number of records: {rdd2}")

def question3(spark):
    paragraph = "Python Lists allow us to hold items of heterogeneous types. In this article, we will learn how to create a list in Python; access the list items; find the number of items in the list, how to add an item to list; how to remove an item from the list; loop through list items; sorting a list, reversing a list; and many more transformation and aggregation actions on Python Lists."
    rdd = spark.sparkContext.parallelize(paragraph.split())
    word_count = (
        rdd.map(lambda word: (word.strip('.,;').lower(),1))
                .reduceByKey(lambda x,y : x+y)
    )

    for word,count in word_count.collect():
        print((word,count))

def question4(spark):
    data  = [("James","","Smith","36636","M",3000),
    ("Michael","Rose","","40288","M",4000),
    ("Robert","","Williams","42114","M",4000),
    ("Maria","Anne","Jones","39192","F",4000),
    ("Jen","Mary","Brown","","F",-1)
  ]
    schema = StructType([ \
        StructField("firstname", StringType(), True), \
        StructField("middlename", StringType(), True), \
        StructField("lastname", StringType(), True), \
        StructField("id", StringType(), True), \
        StructField("gender", StringType(), True), \
        StructField("salary", IntegerType(), True) \
        ])
    df = spark.createDataFrame(data=data, schema=schema)
    df.printSchema()
    df.show(truncate=False)

    df.createOrReplaceTempView('Users')
    result = spark.sql("select * from Users where salary>3000")
    result.show(truncate=False)

def question5(spark):
    structureData = [
        (("James", "", "Smith"), "36636", "M", 3100),
        (("Michael", "Rose", ""), "40288", "M", 4300),
        (("Robert", "", "Williams"), "42114", "M", 1400),
        (("Maria", "Anne", "Jones"), "39192", "F", 5500),
        (("Jen", "Mary", "Brown"), "", "F", -1)
    ]
    structureSchema = StructType([
        StructField('name', StructType([
            StructField('firstname', StringType(), True),
            StructField('middlename', StringType(), True),
            StructField('lastname', StringType(), True)
        ])),
        StructField('id', StringType(), True),
        StructField('gender', StringType(), True),
        StructField('salary', IntegerType(), True)
    ])

    df = spark.createDataFrame(data=structureData, schema=structureSchema)
    df.printSchema()
    df.show(truncate=False)

    df.createOrReplaceTempView('Users')
    result = spark.sql("select name.firstname from Users where name.lastname = 'Rose'")
    result.show(truncate=False)

if __name__ == '__main__':
    question1(spark)
    #question2(spark)
    #question3(spark)
    #question4(spark)
    #question5(spark)

