from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("SanityCheck") \
    .getOrCreate()

data = [("Ashwin", "Kochi", 28),
        ("Rahul", "Bangalore", 25)]

df = spark.createDataFrame(data, ["name", "city", "age"])
df.show()