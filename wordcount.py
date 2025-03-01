from pyspark.sql import SparkSession


def wordcount_example(spark):
    data = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
    rdd = spark.sparkContext.parallelize(data)
    rdd = spark.sparkContext.textFile("file:///home/takeo/pycharmprojects/test.txt")
    rdd2 = rdd.flatMap(lambda x: x.split(" "))
    rdd3 = rdd2.map(lambda x: (x, 1))
    rdd5 = rdd3.reduceByKey(lambda a, b: a + b)
    rdd6 = rdd5.map(lambda x: (x[1], x[0])).sortByKey()
    print(rdd6.collect())

def simple_functions(spark):
    data = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
    rdd1 = spark.sparkContext.parallelize(data)
    print("Count : " + str(rdd1.count()))

    firstRec = rdd1.first()
    print("First Record : " + str(firstRec))

    datMax = rdd1.max()
    print("Max Record : " + str(datMax))

def class_project(spark):
    rdd_category = spark.sparkContext.textFile("file:///home/takeo/pycharmprojects/J_AddressCategory.csv")
    rdd_district = spark.sparkContext.textFile("file:///home/takeo/pycharmprojects/J_AddressDistrict.csv")
    rdd = rdd_category.map(lambda x:x.split(',')).map(lambda x: x[0])
    rdd1 = rdd.map(lambda word:(word, 1)).reduceByKey(lambda x,y : x+y).sortByKey(False)
    top_category = rdd1.takeOrdered(2, key=lambda x: -x[1])
    print(top_category)

    rdadd = rdd_category.map(lambda x: x.split(',')).map(lambda x: x[1])
    rdadd1 = rdadd.map(lambda word: (word, 1)).reduceByKey(lambda x, y: x + y).sortByKey(False)
    top_address = rdadd1.takeOrdered(2, key=lambda x: -x[1])
    print(top_address)

    rddis = rdd_district.map(lambda x: x.split(',')).map(lambda x: x[0])
    rddis1 = rddis.map(lambda word: (word, 1)).reduceByKey(lambda x, y: x + y).sortByKey(False)
    top_district = rddis1.takeOrdered(2, key=lambda x: -x[1])
    print(top_district)

if __name__=='__main__':
    spark:SparkSession = SparkSession.builder.master("local[1]").appName("bootcamp.com").getOrCreate()
    wordcount_example(spark)
    #simple_functions(spark)
    #class_project(spark)