from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder \
    .appName("CSV to Parquet") \
    .getOrCreate()

file_2010_to_2019_path = "hdfs://master:9000/home/user/exercise_datasets/Crime_Data_from_2010_to_2019.csv"
file_2020_to_present_path = "hdfs://master:9000/home/user/exercise_datasets/Crime_Data_from_2020_to_Present.csv" 

# Load CSV files into Spark DataFrames
df1 = spark.read.csv(file_2010_to_2019_path, header=True, inferSchema=True)
df2 = spark.read.csv(file_2020_to_present_path, header=True, inferSchema=True)

# Write the DataFrame to Parquet format
df1.write.parquet("hdfs://master:9000/home/user/exercise_datasets/Crime_Data_from_2010_to_2019.parquet")
df2.write.parquet("hdfs://master:9000/home/user/exercise_datasets/Crime_Data_from_2020_to_Present.parquet")

# Stop the SparkSession
spark.stop()
