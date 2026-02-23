from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("AnalyticsJob").master("local[*]").getOrCreate()

print("Spark Session created successfully.")

df = spark.read.csv("data/synthetic_ecommerce_orders.csv", header=True, inferSchema=True)

print("DataFrame loaded successfully.")

print("Total rows in raw dataset: ", df.count())

# Student roll no based partitioning
student_id = int(input("Enter your student ID: "))
df = df.filter((col("order_id")%10) == (student_id % 10))   
print("Total rows in filtered dataset: ", df.count())

# Initial data quality report metrics
total_rows = df.count()
total_columns = len(df.columns)
column_names = ", ".join(df.columns)
duplicate_count = df.groupBy("order_id").count().filter("count > 1").count()
negative_price_count = df.filter(col("price") < 0).count()

summary_data = [(
    student_id,
    total_rows,
    total_columns,
    column_names,
    duplicate_count,
    negative_price_count
)]

# summary_df = spark.createDataFrame(summary_data,
#                                    schema=["student_id", "total_rows", "total_columns", "column_names", "duplicate_count", "negative_price_count"]
# )

# summary_df.write.mode("overwrite").option("header", True).csv("file:///F:/python_projects/pyspark_tutorial/reports/initial_data_quality_report")

df.groupBy("category").count().show()