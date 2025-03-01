from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, trim, lower

# Initialize Spark Session
spark = SparkSession.builder.appName("TelecomDataProcessing").enableHiveSupport().getOrCreate()

# Read Call Detail Records (CDR) Data from HDFS
cdr_df = spark.read.csv("file:///home/takeo/pycharmprojects/cdr_data.csv", header=True, inferSchema=True)
# # Read Customer Usage Data from HDFS
usage_df = spark.read.csv("file:///home/takeo/pycharmprojects/customer_usage_data.csv", header=True, inferSchema=True)

# Data Cleaning: Remove Duplicates
cdr_df = cdr_df.dropDuplicates()
usage_df = usage_df.dropDuplicates()

# Handle Null Values: Replace NULLs with Default Values
cdr_df = cdr_df.fillna({"call_end_time": "unknown", "call_duration": 0, "call_status": "incomplete"})
usage_df = usage_df.fillna({"data_used_gb": 0.0, "voice_minutes_used": 0, "sms_used": 0})

# Standardize Data Formats: Trim Whitespaces and Convert to Lowercase
cdr_df = cdr_df.withColumn("call_type", trim(lower(col("call_type"))))
usage_df = usage_df.withColumn("total_charges", when(col("total_charges").isNull(), lit(0.0)).otherwise(col("total_charges")))

# Write Cleaned Data to Hive Tables
cdr_df.write.mode("overwrite").saveAsTable("xyz.cleaned_cdr_data")
usage_df.write.mode("overwrite").saveAsTable("xyz.cleaned_usage_data")

# Check Record Counts Between Source and Target
source_cdr_count = cdr_df.count()
target_cdr_count = spark.sql("SELECT COUNT(*) FROM xyz.cleaned_cdr_data").collect()[0][0]

if source_cdr_count == target_cdr_count:
    print("CDR data is complete and consistent.")
else:
    print("Data inconsistency detected in CDR records.")

source_usage_count = usage_df.count()
target_usage_count = spark.sql("SELECT COUNT(*) FROM xyz.cleaned_usage_data").collect()[0][0]

if source_usage_count == target_usage_count:
    print("Customer usage data is complete and consistent.")
else:
    print("Data inconsistency detected in customer usage records.")


