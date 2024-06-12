from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import FloatType
from math import radians, sin, cos, sqrt, atan2
from pyspark.sql import functions as F
from pyspark import SparkContext, SparkConf
from io import StringIO
import csv

# Dataframe implementation
def use_dataframes(file_path_1, file_path_2, stations_file_path):
    # Create SparkSession
    spark = SparkSession.builder \
        .appName("CrimeAnalysis-Dataframes") \
        .getOrCreate()

    # Load crime data using DataFrames and union them
    crime_data1 = spark.read.csv(file_path_1, header=True, inferSchema=True)
    crime_data2 = spark.read.csv(file_path_2, header=True, inferSchema=True)
    crime_df = crime_data1.union(crime_data2)

    # Load the stations data directly as an RDD
    police_df = spark.read.csv(stations_file_path, header=True, inferSchema=True)

    # Filter crimes involving firearms and not in Null islands
    firearm_crimes_df = crime_df.filter(
        (col("Weapon Used Cd").startswith("1")) &
        (col("LAT") != 0) & (col("LON") != 0)
    )

    # Perform broadcast join
    joined_df = firearm_crimes_df.join(
        police_df,
        firearm_crimes_df["AREA "] == police_df["PREC"],
        "inner"
    )

    # Calculate distance between crime location and police department
    def calculate_distance(lat1, lon1, lat2, lon2):
        # Radius of the Earth in kilometers
        R = 6371.0
        
        # Convert latitude and longitude from degrees to radians
        lat1_rad = radians(lat1)
        lon1_rad = radians(lon1)
        lat2_rad = radians(lat2)
        lon2_rad = radians(lon2)
        
        # Calculate differences in latitude and longitude
        dlat = lat2_rad - lat1_rad
        dlon = lon2_rad - lon1_rad
        
        # Calculate distance using Haversine formula
        a = sin(dlat / 2)**2 + cos(lat1_rad) * cos(lat2_rad) * sin(dlon / 2)**2
        c = 2 * atan2(sqrt(a), sqrt(1 - a))
        distance = R * c
        
        return distance

    calculate_distance_udf = F.udf(calculate_distance, FloatType())

    joined_df = joined_df.withColumn(
        "distance_to_police",
        calculate_distance_udf(col("LAT"), col("LON"), col("Y").cast("float"), col("X").cast("float"))
    )

    # Aggregate results by police department
    result_df = joined_df.groupBy("DIVISION") \
        .agg({"distance_to_police": "avg", "*": "count"}) \
        .withColumnRenamed("count(1)", "incidents_total") \
        .withColumnRenamed("avg(distance_to_police)", "average_distance") \
        .orderBy(col("incidents_total").desc())

    # Show results
    result_df.show(result_df.count(), truncate=False)

    # Stop SparkSession
    spark.stop()


# Rdd broadcast join implementation
def use_rdd_broadcast_join(file_path_1, file_path_2, stations_file_path):
    spark = SparkSession.builder \
        .appName("CrimeAnalysis-RddBroadcastJoin") \
        .getOrCreate()

    sc = spark.sparkContext

    def parse_csv(line):
        if not line.strip():
            return None
        sio = StringIO(line)
        reader = csv.reader(sio)
        return next(reader)

    crime_data1 = sc.textFile(file_path_1)
    crime_data2 = sc.textFile(file_path_2)
    police_rdd = sc.textFile(stations_file_path)

    header_crime1 = crime_data1.first()
    header_crime2 = crime_data2.first()

    crime_data1 = crime_data1.filter(lambda row: row != header_crime1).map(parse_csv).filter(lambda x: x is not None)
    crime_data2 = crime_data2.filter(lambda row: row != header_crime2).map(parse_csv).filter(lambda x: x is not None)
    crime_rdd = crime_data1.union(crime_data2)

    header_police = police_rdd.first()
    police_rdd = police_rdd.filter(lambda row: row != header_police).map(parse_csv)

    filtered_crime_rdd = crime_rdd.filter(lambda row: row[16].startswith('1') and row[-2] != 0 and row[-1] != 0)

    # Convert to key-value pairs
    filtered_crime_rdd = filtered_crime_rdd.map(lambda row: (row[4].strip().lstrip('0'), row))
    police_rdd = police_rdd.map(lambda row: (row[5], row))

    # Broadcast police data
    broadcast_police = sc.broadcast(police_rdd.collectAsMap())

    # Filter and join
    def filter_and_join(record):
        area, crime = record
        police_data = broadcast_police.value.get(area)
        if police_data:
            try:
                lat1, lon1 = float(crime[-2]), float(crime[-1])  # LAT and LON
                lat2, lon2 = float(police_data[1]), float(police_data[0])  # Y and X

                if lat1 != 0 and lon1 != 0 and lat2 != 0 and lon2 != 0:
                    return (police_data[3], (lat1, lon1, lat2, lon2, 1))  # DIVISION
            except ValueError:
                print(f"ValueError for area: {area}")
        else:
            print(f"No match for area: {area}")
        return None

    filtered_joined_rdd = filtered_crime_rdd.map(filter_and_join).filter(lambda x: x is not None)

    # Calculate distance
    def calculate_distance(data):
        division, (lat1, lon1, lat2, lon2, count) = data
        R = 6371.0
        lat1_rad, lon1_rad = radians(lat1), radians(lon1)
        lat2_rad, lon2_rad = radians(lat2), radians(lon2)
        dlat, dlon = lat2_rad - lat1_rad, lon2_rad - lon1_rad
        a = sin(dlat / 2)**2 + cos(lat1_rad) * cos(lat2_rad) * sin(dlon / 2)**2
        c = 2 * atan2(sqrt(a), sqrt(1 - a))
        distance = R * c
        return (division, (distance, count))

    distance_rdd = filtered_joined_rdd.map(calculate_distance)

    # Aggregate results
    result_rdd = distance_rdd.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])) \
        .mapValues(lambda x: (x[0] / x[1], x[1])) \
        .sortBy(lambda x: x[1][1], ascending=False)

    # Collect and show results
    results = result_rdd.collect()
    for result in results:
        division, (avg_distance, incidents_total) = result
        print(f"Division: {division}, Average Distance: {avg_distance}, Total Incidents: {incidents_total}")

    # Stop SparkContext
    spark.stop()


# Repartition join
def use_rdd_repartition_join(file_path_1, file_path_2, stations_file_path):
    spark = SparkSession.builder \
        .appName("CrimeAnalysis-RddRepartitionJoin") \
        .getOrCreate()

    sc = spark.sparkContext

    def parse_csv(line):
        if not line.strip():
            return None
        sio = StringIO(line)
        reader = csv.reader(sio)
        return next(reader)

    crime_data1 = sc.textFile(file_path_1)
    crime_data2 = sc.textFile(file_path_2)
    police_rdd = sc.textFile(stations_file_path)

    header_crime1 = crime_data1.first()
    header_crime2 = crime_data2.first()

    crime_data1 = crime_data1.filter(lambda row: row != header_crime1).map(parse_csv).filter(lambda x: x is not None)
    crime_data2 = crime_data2.filter(lambda row: row != header_crime2).map(parse_csv).filter(lambda x: x is not None)
    crime_rdd = crime_data1.union(crime_data2)

    header_police = police_rdd.first()
    police_rdd = police_rdd.filter(lambda row: row != header_police).map(parse_csv)

    filtered_crime_rdd = crime_rdd.filter(lambda row: row[16].startswith('1') and row[-2] != 0 and row[-1] != 0)

    # Convert to key-value pairs
    filtered_crime_rdd = filtered_crime_rdd.map(lambda row: (row[4].strip().lstrip('0'), ('R', row)))
    police_rdd = police_rdd.map(lambda row: (row[5], ('L', row)))

    # Repartition join
    joined_rdd = filtered_crime_rdd.union(police_rdd) \
        .groupByKey() \
        .flatMapValues(lambda records: [(r, l) for r in records if r[0] == 'R' for l in records if l[0] == 'L'])

    # Filter and join
    def filter_and_join(record):
        ((tag1, crime), (tag2, police)) = record[1]
        try:
            lat1, lon1 = float(crime[-2]), float(crime[-1])  # LAT and LON
            lat2, lon2 = float(police[1]), float(police[0])  # Y and X

            if lat1 != 0 and lon1 != 0 and lat2 != 0 and lon2 != 0:
                return (police[3], (lat1, lon1, lat2, lon2, 1))  # DIVISION
        except ValueError:
            print(f"ValueError for record: {record}")
        return None

    filtered_joined_rdd = joined_rdd.map(filter_and_join).filter(lambda x: x is not None)

    # Calculate distance
    def calculate_distance(data):
        division, (lat1, lon1, lat2, lon2, count) = data
        R = 6371.0
        lat1_rad, lon1_rad = radians(lat1), radians(lon1)
        lat2_rad, lon2_rad = radians(lat2), radians(lon2)
        dlat, dlon = lat2_rad - lat1_rad, lon2_rad - lon1_rad
        a = sin(dlat / 2)**2 + cos(lat1_rad) * cos(lat2_rad) * sin(dlon / 2)**2
        c = 2 * atan2(sqrt(a), sqrt(1 - a))
        distance = R * c
        return (division, (distance, count))

    distance_rdd = filtered_joined_rdd.map(calculate_distance)

    # Aggregate results
    result_rdd = distance_rdd.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])) \
        .mapValues(lambda x: (x[0] / x[1], x[1])) \
        .sortBy(lambda x: x[1][1], ascending=False)

    # Collect and show results
    results = result_rdd.collect()
    for result in results:
        division, (avg_distance, incidents_total) = result
        print(f"Division: {division}, Average Distance: {avg_distance}, Total Incidents: {incidents_total}")

    # Stop SparkContext
    spark.stop()


file_path_1 = "hdfs://master:9000/home/user/exercise_datasets/Crime_Data_from_2010_to_2019.csv"
file_path_2 = "hdfs://master:9000/home/user/exercise_datasets/Crime_Data_from_2020_to_Present.csv"
stations_file_path = "hdfs://master:9000/home/user/exercise_datasets/LAPD_Police_Stations_-3946316159051949741.csv"

use_dataframes(file_path_1, file_path_2, stations_file_path)
use_rdd_broadcast_join(file_path_1, file_path_2, stations_file_path)
use_rdd_repartition_join(file_path_1, file_path_2, stations_file_path)