from pyspark.sql import SparkSession

# read from postgres and write to HIVE
appName = 'ex2'
spark = SparkSession.builder \
	.appName(appName) \
	.enableHiveSupport() \
	.getOrCreate()

df = spark.read.jdbc(\
    "jdbc:postgresql://vlt-ace-postg.dhe.duke.edu/dukehealth_dwh",\
    "public.sp500_csv",\
    properties={"user": "dwgadmin", "password": "dwgadmin"})

df.show()

# this is use default derby, which show up at spark-master /spark-warehouse/test_table
df.write.mode("overwrite").saveAsTable("default.test_table")

