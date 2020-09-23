from pyspark.sql import SparkSession

spark=SparkSession.builder.appName("ex1").getOrCreate()

df =  spark.createDataFrame(
  [(1, "Bob"),
   (2, "Bob"),
   (3, "Collin"),
   (4, "Alice"),
   (5, "Alice"),
   (6, "Alice"),
   (7, None),
   (8, None),
  ],
  ("id", "name"))
df.show()

jdbcDF2 = spark.read.jdbc(\
    "jdbc:postgresql://vlt-ace-postg.dhe.duke.edu/dukehealth_dwh",\
    "public.sp500_csv",\
    properties={"user": "dwgadmin", "password": "dwgadmin"})

jdbcDF2.show()
