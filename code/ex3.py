from pyspark.sql import SparkSession

# read from postgres and write to json
appName = 'ex3'
spark = SparkSession.builder \
	.appName(appName) \
	.enableHiveSupport() \
	.getOrCreate()

df = spark.read.jdbc(\
    "jdbc:postgresql://vlt-ace-postg.dhe.duke.edu/dukehealth_dwh",\
    "public.sp500_csv",\
    properties={"user": "dwgadmin", "password": "dwgadmin"})

df.show()

# this will save to /tmp/sp500.json folder of spark-master node
df.write.mode("overwrite").json("/tmp/sp500.json")

# hdfs://host:port, this write to hdfs namenode root dir
df.write.mode("overwrite").format("json").save("hdfs://namenode:8020/sp500.json")

