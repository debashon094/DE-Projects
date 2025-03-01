from pyspark.shell import spark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, ArrayType
from pyspark.sql.functions import col,lit

def df_intro(spark):
    data = [("James", "Smith", "USA", "CA"),
            ("Michael", "Rose", "USA", "NY"),
            ("Robert", "Williams", "USA", "CA"),
            ("Maria", "Jones", "USA", "FL")
            ]
    columns = ["firstname", "lastname", "country", "state"]
    df = spark.createDataFrame(data=data, schema=columns)
    df.show(truncate=False)

def singCol(spark):
    data = [
        (("James", None, "Smith"), "OH", "M"),
        (("Anna", "Rose", ""), "NY", "F"),
        (("Julia", "", "Williams"), "OH", "F"),
        (("Maria", "Anne", "Jones"), "NY", "M"),
        (("Jen", "Mary", "Brown"), "NY", "M"),
        (("Mike", "Mary", "Williams"), "OH", "M")
    ]
    schema = StructType([
        StructField('name', StructType([
            StructField('firstname', StringType(), True),
            StructField('middlename', StringType(), True),
            StructField('lastname', StringType(), True)
        ])),
        StructField('state', StringType(), True),
        StructField('gender', StringType(), True)
    ])
    df2 = spark.createDataFrame(data=data, schema=schema)
    df2.printSchema()
    df2.select("name.firstname","name.lastname").show(truncate=False)

def withColumn(spark):
    data = [('James', '', 'Smith', '1991-04-01', 'M', 3000),
            ('Michael', 'Rose', '', '2000-05-19', 'M', 4000),
            ('Robert', '', 'Williams', '1978-09-05', 'M', 4000),
            ('Maria', 'Anne', 'Jones', '1967-12-01', 'F', 4000),
            ('Jen', 'Mary', 'Brown', '1980-02-17', 'F', -1)
            ]
    columns = ["firstname", "middlename", "lastname", "dob", "gender", "salary"]

    df = spark.createDataFrame(data=data, schema=columns)
    df.printSchema()
    df.show(truncate=False)
    ddf = df.withColumn("salary",col("salary").cast("Double"))
    ddf.printSchema()
    ddf.show(truncate=False)
    ddf.withColumn("gender",lit("M")).show()
    ddf.withColumnRenamed("gender","sex").show(truncate=False)

def filter(spark):
    data = [
        (("James", "", "Smith"), ["Java", "Scala", "C++"], "OH", "M"),
        (("Anna", "Rose", ""), ["Spark", "Java", "C++"], "NY", "F"),
        (("Julia", "", "Williams"), ["CSharp", "VB"], "OH", "F"),
        (("Maria", "Anne", "Jones"), ["CSharp", "VB"], "NY", "M"),
        (("Jen", "Mary", "Brown"), ["CSharp", "VB"], "NY", "M"),
        (("Mike", "Mary", "Williams"), ["Python", "VB"], "OH", "M")
    ]
    schema = StructType([
        StructField('name', StructType([
            StructField('firstname', StringType(), True),
            StructField('middlename', StringType(), True),
            StructField('lastname', StringType(), True)
        ])),
        StructField('languages', ArrayType(StringType()), True),
        StructField('state', StringType(), True),
        StructField('gender', StringType(), True)
    ])
    df = spark.createDataFrame(data=data, schema=schema)
    df.printSchema()
    df.show(truncate=False)

    df.filter(df.state == "OH").show(truncate=False)
    li = ["OH","CA","DE"]
    df.filter(df.state.isin(li)).show()

    # using startswith
    df.filter(df.state.isin(li)==False).show()

    # using endswith
    df.filter(df.state.endswith("H")).show()

    # contains
    df.filter(df.state.contains("H")).show()


def filter_like(spark):
    data2 = [(2, "Michael Rose"), (3, "Robert Williams"),
             (4, "Rames Rose"), (5, "Rames rose")
             ]
    df2 = spark.createDataFrame(data=data2, schema=["id", "name"])

    # like - SQL LIKE pattern
    df2.filter(df2.name.like("%rose%")).show()


if __name__=='__main__':
    #df_intro(spark)
    #singCol(spark)
    #withColumn(spark)
    #filter(spark)
    filter_like(spark)
