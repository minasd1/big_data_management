from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, concat_ws
from pyspark.sql.window import Window
from pyspark.sql.functions import col, date_format, row_number
from datetime import datetime
from pyspark.sql.functions import to_date, col
import time
from pyspark.sql import functions as F

print("Map reduce function starts:\n")

def use_sql(crime_data):
        crime_data.createOrReplaceTempView("crime_data")

        query_start_time = time.time()
        crime_dataframe = spark.sql("""
        SELECT 
            year,
            month,
            crime_total,
            rank
        FROM (
            SELECT 
                YEAR(to_date(`Date Rptd`, 'MM/dd/yyyy')) as year,
                MONTH(to_date(`Date Rptd`, 'MM/dd/yyyy')) as month,
                COUNT(*) as crime_total,
                ROW_NUMBER() OVER (PARTITION BY YEAR(to_date(`Date Rptd`, 'MM/dd/yyyy')) ORDER BY COUNT(*) DESC) as rank
            FROM 
                crime_data
            GROUP BY 
                year, month
        ) t
           WHERE rank <= 3
               ORDER BY 
                    year, rank
        """)
        query_end_time = time.time()
        print(f"Time to execute query: {query_end_time - query_start_time:.2f} seconds")
        crime_dataframe.show(crime_dataframe.count(), truncate=False)
        show_time = time.time()
        print(f"Time to show results: {show_time - query_end_time:.2f} seconds")

        total_time = show_time - start_time
        print(f"Total execution time: {total_time:.2f} seconds")

def use_dataframe(crime_data):
        start_time = time.time()
        read_csv_time = time.time()
        print(f"Time to read CSV: {read_csv_time - start_time:.2f} seconds")
        convert_date_time = time.time()
        print(f"Time to convert date: {convert_date_time - read_csv_time:.2f} seconds")

        dataframe_start_time = time.time()

        convert_date_time = time.time()
        print(f"Time to convert date: {convert_date_time - read_csv_time:.2f} seconds")

        crime_df = crime_data.withColumn('date_rec', F.to_date(F.col('Date Rptd'), 'MM/dd/yyyy'))
        crime_df = crime_df.withColumn('year', F.year('date_rec')).withColumn('month', F.month('date_rec'))

        # Group by year and month, and count the number of crimes
        crime_counts = crime_df.groupBy('year', 'month').agg(F.count('*').alias('crime_total'))

        # Define a window specification to rank months within each year by crime total in descending order
        window_spec = Window.partitionBy('year').orderBy(F.desc('crime_total'))

        # Add a rank column based on the window specification
        crime_ranked = crime_counts.withColumn('rank', F.row_number().over(window_spec))

        # Filter to get only the top 3 months per year
        top_crimes = crime_ranked.filter(F.col('rank') <= 3)

        # Order by year and rank
        result_df = top_crimes.orderBy('year', 'rank')

        # Show the result
        result_df.show(result_df.count(), truncate=False)
        filtering_date_time = time.time()
        print(f"Time to filter: {filtering_date_time-convert_date_time :.2f} seconds")

        print("\n")

        query_end_time = time.time()
        print(f"Time to execute calculations: {query_end_time - dataframe_start_time :.2f} seconds")
        show_time = time.time()
        print(f"Time to show results: {show_time - query_end_time:.2f} seconds")

        total_time = show_time - start_time
        print(f"Total execution time: {total_time:.2f} seconds")

# Read data depending their type (csv or parquet)
def read_data(file1, file2, type):
        read_csv_time = time.time()
        if type == "csv":
            crime_data1 = spark.read.csv(file1, header=True, inferSchema=True)
            crime_data2 = spark.read.csv(file2, header=True, inferSchema=True)
        elif type == "parquet":
            crime_data1 = spark.read.parquet(file1)
            crime_data2 = spark.read.parquet(file2)
        df = crime_data1.union(crime_data2)
        print(f"Time to read {type}: {read_csv_time - start_time:.2f} seconds")
        return df

# Start a spark session
spark = SparkSession.builder \
    .appName("Query1") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .getOrCreate()

start_time = time.time()

#Csv files
file_path_1_csv="hdfs://master:9000/home/user/exercise_datasets/Crime_Data_from_2010_to_2019.csv"
file_path_2_csv="hdfs://master:9000/home/user/exercise_datasets/Crime_Data_from_2020_to_Present.csv"

#Parquet files
file_path_1_parquet="hdfs://master:9000/home/user/exercise_datasets/Crime_Data_from_2010_to_2019.parquet"
file_path_2_parquet="hdfs://master:9000/home/user/exercise_datasets/Crime_Data_from_2020_to_Present.parquet"

# Csv implementations
df_csv=read_data(file_path_1_csv, file_path_2_csv, "csv")
use_sql(df_csv)
use_dataframe(df_csv)

# Parquet implementations
df_parquet = read_data(file_path_1_parquet, file_path_2_parquet, "parquet")
use_sql(df_parquet)
use_dataframe(df_parquet)

spark.stop()