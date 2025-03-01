from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, trim, lower

# Initialize Spark Session
spark = SparkSession.builder.appName("EcommerceDataProcessing").enableHiveSupport().getOrCreate()

# Read Customer Data from HDFS
customer_df = spark.read.csv("file:///home/takeo/pycharmprojects/customer_data.csv", header=True, inferSchema=True)


# Read Sales Transactions Data from HDFS
sales_df = spark.read.csv("file:///home/takeo/pycharmprojects/sale_data.csv", header=True, inferSchema=True)

#data cleaning remove duplicates
customer_df = customer_df.dropDuplicates()
sales_df = sales_df.dropDuplicates()

# Handle Null Values: Replace NULLs with Default Values
customer_df = customer_df.fillna({"first_name": "Unknown", "phone": "Unknown", "address": "Unknown", "total_spent": 0.0})
sales_df = sales_df.fillna({'quantity': 0, 'price': 0.0})


# Standardize Data Formats: Trim Whitespaces and Convert to Lowercase
customer_df = customer_df.withColumn("email", trim(lower(col("email"))))
sales_df = sales_df.withColumn("payment_method", trim(lower(col("payment_method"))))

# Write Cleaned Data to Hive Tables
customer_df.write.mode("overwrite").saveAsTable("xyz.cleaned_customer_data")
sales_df.write.mode("overwrite").saveAsTable("xyz.cleaned_sales_data")

# Check Record Counts Between Source and Target
source_customer_count = customer_df.count()
target_customer_count = spark.sql("SELECT COUNT(*) FROM xyz.cleaned_customer_data").collect()[0][0]

if source_customer_count == target_customer_count:
    print("Customer data is complete and consistent.")
else:
    print("Data inconsistency detected in customer records.")

source_sales_count = sales_df.count()
target_sales_count = spark.sql("SELECT COUNT(*) FROM xyz.cleaned_sales_data").collect()[0][0]

if source_sales_count == target_sales_count:
    print("Sales data is complete and consistent.")
else:
    print("Data inconsistency detected in sales records.")
