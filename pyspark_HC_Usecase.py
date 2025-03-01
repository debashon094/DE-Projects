from os import truncate
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when
spark = SparkSession.builder.appName("HealthcareDataProcessing").enableHiveSupport().getOrCreate()

def pysparkusecase(spark):
    patient_df = spark.read.csv("file:///home/takeo/pycharmprojects/patients_data.csv", header=True, inferSchema=True)

    claims_df = spark.read.csv("file:///home/takeo/pycharmprojects/claim_data.csv", header=True, inferSchema=True)
    patient_df = patient_df.dropDuplicates()
    claims_df = claims_df.dropDuplicates()

    patient_df = patient_df.fillna({"first_name": "Unknown", "address": "Unknown", "insurance_plan_id": -1})
    claims_df = claims_df.fillna({"claim_amount": 0.0})
    patient_df.show(truncate=False)
    claims_df.show(truncate=False)
    patient_df.write.mode("overwrite").saveAsTable("xyz.cleaned_patient_data")
    claims_df.write.mode("overwrite").saveAsTable("xyz.cleaned_claims_data")
    source_patient_count = patient_df.count()
    target_patient_count = spark.sql("SELECT COUNT(*) FROM xyz.cleaned_patient_data").collect()[0][0]
    if source_patient_count == target_patient_count:
        print("Patient data is complete and consistent.")
    else:
        print("Data inconsistency detected in patient records.")

    source_claims_count = claims_df.count()
    target_claims_count = spark.sql("SELECT COUNT(*) FROM xyz.cleaned_claims_data").collect()[0][0]

    if source_claims_count == target_claims_count:
        print("Claims data is complete and consistent.")
    else:
        print("Data inconsistency detected in claims records.")


if __name__ == '__main__':
    pysparkusecase(spark)