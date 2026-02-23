from pyspark.sql import SparkSession

# appName = labels the job
# master = tells to use all the cores of the local machine
# getOrCreate() = creates a new SparkSession or returns an existing one
spark = SparkSession.builder.appName("AnalyticsJob").master("local[*]").getOrCreate()

# Nothing is computed. Full data is not loaded into memory.
# Spark uses lazy evaluation, which means that it builds a logical execution plan but does not execute it until an action is called.
df = spark.read.csv("synthetic_ecommerce_orders.csv", header=True, inferSchema=True)

# this is an action that triggers the execution of the lazy evaluation and loads the data
df.show(5)

#Transformations V/S Actions
# Transformations are operations that create a new DataFrame from an existing one. They are lazy and do not trigger execution until an action is called.
# Examples of transformations include filter(), select(), groupBy(), and withColumn().  
# Actions are operations that trigger the execution of the transformations and return a result. They are eager and cause the data to be processed.
# Examples of actions include show(), collect(), count(), and write().


# this will tell how many partitions the data is split into.
print(df.rdd.getNumPartitions())


# this will show the execution plan i.e. the DAG spark creates.
# Its important to learn how to read a physical plan of Spark.
df.filter("price>1000").explain()