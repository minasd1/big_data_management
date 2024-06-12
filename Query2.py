from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, when
from pyspark.sql.functions import to_date, col
import time
from pyspark.sql import Row


def rdd_query2(crime_data):
    start_time = time.time()

    # Convert DataFrame to RDD of Rows
    crime_rdd = crime_data.rdd

    # Define a function to categorize the time of day
    def categorize_time_occ(time_occ):
        if 500 <= time_occ < 1159:
            return "Morning"
        elif 1200 <= time_occ < 1659:
            return "Afternoon"
        elif 1700 <= time_occ < 2059:
            return "Evening"
        else:
            return "Night"

    # Perform data transformations using map
    transformed_rdd = crime_rdd.map(lambda row: (categorize_time_occ(row["TIME OCC"]), row["Premis Desc"]))

    # Filter RDD to include only STREET crimes
    filtered_rdd = transformed_rdd.filter(lambda x: x[1] == "STREET")

    # Perform aggregation using reduceByKey
    counts_rdd = filtered_rdd.map(lambda x: (x[0], 1)).reduceByKey(lambda a, b: a + b)

    # Sort the RDD in descending order based on the count
    sorted_counts_rdd = counts_rdd.sortBy(lambda x: x[1], ascending=False)

    # Collect aggregated results
    part_of_day_counts = sorted_counts_rdd.collect()

    # Print results
    for part_of_day, count in part_of_day_counts:
        print(f"{part_of_day}: {count}")

    # Calculate and print execution times
    query_end_time = time.time()
    print(f"Time to execute query: {query_end_time - start_time:.2f} seconds")
    total_time = query_end_time - start_time
    print(f"Total execution time: {total_time:.2f} seconds")


def sql_query2(crime_data):
    start_time = time.time()
    crime_data= crime_data.withColumn(
    "crime_time",
        expr("""
        CASE
            WHEN `Time Occ` >= 500 AND `Time Occ` < 1159 THEN 'Morning'
            WHEN `Time Occ` >= 1200 AND `Time Occ` < 1659 THEN 'Afternoon'
            WHEN `Time Occ` >= 1700 AND `Time Occ` < 2059 THEN 'Evening'
            ELSE 'Night'
        END
        """)
    )
    crime_data.createOrReplaceTempView("crime_data")
    query_start_time = time.time()
    sql_df = spark.sql("""
    SELECT 
        crime_time,
        COUNT(*) as count
    FROM 
        crime_data
    WHERE
        `Premis Desc` = 'STREET'
    GROUP BY 
        crime_time
    ORDER BY 
        count DESC
    """)
    query_end_time = time.time()
    sql_df.show()
    show_time = time.time()
    print(f"Time to execute query: {query_end_time - query_start_time:.2f} seconds")
    print(f"Time to show results: {show_time - query_end_time:.2f} seconds")
    total_time = show_time - start_time
    print(f"Total execution time: {total_time:.2f} seconds")

# Create the Spark session with memory allocation
spark = SparkSession.builder \
    .appName("Query2") \
    .getOrCreate()  

file_path_1="hdfs://master:9000/home/user/exercise_datasets/Crime_Data_from_2010_to_2019.csv"
crime_data1 = spark.read.csv(file_path_1, header=True, inferSchema=True)
file_path_2="hdfs://master:9000/home/user/exercise_datasets/Crime_Data_from_2020_to_Present.csv"
crime_data2 = spark.read.csv(file_path_2, header=True, inferSchema=True)
df = crime_data1.union(crime_data2)

rdd_query2(df)
sql_query2(df)


spark.stop()