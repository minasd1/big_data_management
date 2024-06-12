from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from pyspark.sql.functions import expr
from pyspark.sql.functions import broadcast
from pyspark.sql.functions import col, desc
from pyspark.sql.functions import to_date, col, regexp_replace
from pyspark.sql import functions as F
# import pandas as pd
import time


def use_join():
    spark = SparkSession.builder \
        .appName("CrimeDataFilter-SimpleJoin") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .getOrCreate()

    file_path = "hdfs://master:9000/home/user/exercise_datasets/LA_income_2015.csv"
    table1 = spark.read.csv(file_path, header=True, inferSchema=True)

    file_path = "hdfs://master:9000/home/user/exercise_datasets/Crime_Data_from_2010_to_2019.csv"
    crime_data1 = spark.read.csv(file_path, header=True, inferSchema=True)
    file_path = "hdfs://master:9000/home/user/exercise_datasets/Crime_Data_from_2020_to_Present.csv"
    crime_data2 = spark.read.csv(file_path, header=True, inferSchema=True)
    table2 = crime_data1.union(crime_data2)

    crime_2015 = table2.withColumn('Date Rptd', F.to_timestamp(F.col('Date Rptd'), 'MM/dd/yyyy hh:mm:ss a'))

    # Filter the records for the year 2015
    crime_2015 = crime_2015.filter(F.year(F.col('Date Rptd')) == 2015)

    selected_columns = ["Date Rptd", "LAT", "LON", "Vict Descent"]
    table2 = crime_2015.select(*selected_columns)

    file_path = "hdfs://master:9000/home/user/exercise_datasets/revgecoding.csv"
    table3 = spark.read.csv(file_path, header=True, inferSchema=True)

    table2 = table2.withColumn(
        "Descent",
        F.expr("""
               CASE
                   WHEN trim(`Vict Descent`) == "B" THEN 'Black'
                   WHEN trim(`Vict Descent`) == "W" THEN 'White'
                   WHEN trim(`Vict Descent`) == "A" THEN 'Other Asian'
                   WHEN trim(`Vict Descent`) == "C" THEN 'Chinese'
                   WHEN trim(`Vict Descent`) == "D" THEN 'Cambodian'
                   WHEN trim(`Vict Descent`) == "F" THEN 'Filipino'
                   WHEN trim(`Vict Descent`) == "G" THEN 'Guamanian'
                   WHEN trim(`Vict Descent`) == "H" THEN 'Hispanic/Latin/Mexican'
                   WHEN trim(`Vict Descent`) == "J" THEN 'Japanese'
                   WHEN trim(`Vict Descent`) == "K" THEN 'Korean'
                   WHEN trim(`Vict Descent`) == "L" THEN 'Laotian'
                   WHEN trim(`Vict Descent`) == "P" THEN 'Pacific'
                   WHEN trim(`Vict Descent`) == "S" THEN 'Samoan'
                   WHEN trim(`Vict Descent`) == "U" THEN 'Hawaiian'
                   WHEN trim(`Vict Descent`) == "V" THEN 'Vietnamese'
                   WHEN trim(`Vict Descent`) == "Z" THEN 'Asian Indian'
                   WHEN trim(`Vict Descent`) == "X" THEN 'Unknown'
                   WHEN trim(`Vict Descent`) == "I" THEN 'American Indian/Alaskan Native'
                   WHEN trim(`Vict Descent`) == "O" THEN 'Other'
                   ELSE 'None'
               END
           """)
    )


    join_table = table3.join(table2, (table3["LAT"] == table2["LAT"]) & (table3["LON"] == table2["LON"]), "inner")
    final_table = join_table.join(table1, (join_table["ZIPcode"] == table1["Zip Code"]), "inner")
    final_table.explain()

    # SQL query για να βρούμε τα 3 ZIP codes με το μεγαλύτερο μέσο εισόδημα
    final_table = final_table.withColumn(
        "Estimated Median Income",
        regexp_replace(col("Estimated Median Income"), "[$,]", "").cast("double")
    )
    final_table.createOrReplaceTempView("final_table")
    top_zipcodes = spark.sql("""
        SELECT ZIPcode, AVG(`Estimated Median Income`) as avg_income
        FROM final_table
        GROUP BY ZIPcode
        ORDER BY avg_income DESC
        LIMIT 3
    """)
    top_zipcodes.createOrReplaceTempView("top_zipcodes")


    # Join τον αρχικό πίνακα με τα τρία κορυφαία ZIP codes
    top_zipcodes_joined = spark.sql("""
        SELECT ft.*
        FROM final_table ft
        INNER JOIN top_zipcodes tz
        ON ft.ZIPcode = tz.ZIPcode
    """)
    top_zipcodes_joined.createOrReplaceTempView("top_zipcodes_joined")


    # Υπολογισμός της συχνότητας εμφάνισης της κάθε τιμής στη στήλη "Descent", με φίλτρο για να αποκλείσουμε τις τιμές "Other" και "None"
    descent_counts = spark.sql("""
        SELECT Descent, COUNT(*) as count
        FROM top_zipcodes_joined
        WHERE  Descent != 'None'
        GROUP BY Descent
        ORDER BY count DESC
    """)

    # Εκτύπωση των αποτελεσμάτων
    descent_counts.show()

    # SQL query για να βρούμε τα 3 ZIP codes με το μικρότερο μέσο εισόδημα
    bottom_zipcodes = spark.sql("""
        SELECT `ZIPcode`, AVG(`Estimated Median Income`) as avg_income
        FROM final_table
        GROUP BY `ZIPcode`
        ORDER BY avg_income ASC
        LIMIT 3
    """)
    bottom_zipcodes.createOrReplaceTempView("bottom_zipcodes")

    # Join τον αρχικό πίνακα με τα τρία κορυφαία ZIP codes
    bottom_zipcodes_joined = spark.sql("""
        SELECT ft.*
        FROM final_table ft
        INNER JOIN bottom_zipcodes bz
        ON ft.ZIPcode = bz.ZIPcode
    """)
    bottom_zipcodes_joined.createOrReplaceTempView("bottom_zipcodes_joined")


    # Υπολογισμός της συχνότητας εμφάνισης της κάθε τιμής στη στήλη "Descent", με φίλτρο για να αποκλείσουμε τις τιμές "Other" και "None"
    descent_counts = spark.sql("""
        SELECT Descent, COUNT(*) as count
        FROM bottom_zipcodes_joined
        WHERE Descent NOT IN ('None')
        GROUP BY Descent
        ORDER BY count DESC
    """)

    # Εκτύπωση των αποτελεσμάτων
    descent_counts.show()

    spark.stop()


def use_broadcast():
    spark = SparkSession.builder \
        .appName("CrimeDataFilter-BroadcastJoin") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .getOrCreate()

    file_path = "hdfs://master:9000/home/user/exercise_datasets/LA_income_2015.csv"
    table1 = spark.read.csv(file_path, header=True, inferSchema=True)

    file_path = "hdfs://master:9000/home/user/exercise_datasets/Crime_Data_from_2010_to_2019.csv"
    crime_data1 = spark.read.csv(file_path, header=True, inferSchema=True)
    file_path = "hdfs://master:9000/home/user/exercise_datasets/Crime_Data_from_2020_to_Present.csv"
    crime_data2 = spark.read.csv(file_path, header=True, inferSchema=True)
    table2 = crime_data1.union(crime_data2)

    crime_2015 = table2.withColumn('Date Rptd', F.to_timestamp(F.col('Date Rptd'), 'MM/dd/yyyy hh:mm:ss a'))

    # Filter the records for the year 2015
    crime_2015 = crime_2015.filter(F.year(F.col('Date Rptd')) == 2015)



    selected_columns = ["Date Rptd","LAT", "LON", "Vict Descent"]
    table2 = crime_2015.select(*selected_columns)


    file_path = "hdfs://master:9000/home/user/exercise_datasets/revgecoding.csv"
    table3 = spark.read.csv(file_path, header=True, inferSchema=True)

    table2 = table2.withColumn(
        "Descent",
        F.expr("""
            CASE
                WHEN trim(`Vict Descent`) == "B" THEN 'Black'
                WHEN trim(`Vict Descent`) == "W" THEN 'White'
                WHEN trim(`Vict Descent`) == "A" THEN 'Other Asian'
                WHEN trim(`Vict Descent`) == "C" THEN 'Chinese'
                WHEN trim(`Vict Descent`) == "D" THEN 'Cambodian'
                WHEN trim(`Vict Descent`) == "F" THEN 'Filipino'
                WHEN trim(`Vict Descent`) == "G" THEN 'Guamanian'
                WHEN trim(`Vict Descent`) == "H" THEN 'Hispanic/Latin/Mexican'
                WHEN trim(`Vict Descent`) == "J" THEN 'Japanese'
                WHEN trim(`Vict Descent`) == "K" THEN 'Korean'
                WHEN trim(`Vict Descent`) == "L" THEN 'Laotian'
                WHEN trim(`Vict Descent`) == "P" THEN 'Pacific'
                WHEN trim(`Vict Descent`) == "S" THEN 'Samoan'
                WHEN trim(`Vict Descent`) == "U" THEN 'Hawaiian'
                WHEN trim(`Vict Descent`) == "V" THEN 'Vietnamese'
                WHEN trim(`Vict Descent`) == "Z" THEN 'Asian Indian'
                WHEN trim(`Vict Descent`) == "X" THEN 'Unknown'
                WHEN trim(`Vict Descent`) == "I" THEN 'American Indian/Alaskan Native'
                WHEN trim(`Vict Descent`) == "O" THEN 'Other'
                ELSE 'None'
            END
        """)
    )

    #-------------------

    join_table= table3.join(broadcast(table2), (table3["LAT"] == table2["LAT"]) & (table3["LON"] == table2["LON"]), "inner")
    #------------------
    final_table=table1.hint("broadcast").join(join_table, table1["Zip Code"] == join_table["ZIPcode"], "inner")
    #-------------------

    # SQL query για να βρούμε τα 3 ZIP codes με το μεγαλύτερο μέσο εισόδημα
    final_table = final_table.withColumn(
        "Estimated Median Income",
        regexp_replace(col("Estimated Median Income"), "[$,]", "").cast("double")
    )
    final_table.createOrReplaceTempView("final_table")
    top_zipcodes = spark.sql("""
        SELECT ZIPcode, AVG(`Estimated Median Income`) as avg_income
        FROM final_table
        GROUP BY ZIPcode
        ORDER BY avg_income DESC
        LIMIT 3
    """)
    top_zipcodes.createOrReplaceTempView("top_zipcodes")


    # Join τον αρχικό πίνακα με τα τρία κορυφαία ZIP codes
    top_zipcodes_joined = spark.sql("""
        SELECT ft.*
        FROM final_table ft
        INNER JOIN top_zipcodes tz
        ON ft.ZIPcode = tz.ZIPcode
    """)
    top_zipcodes_joined.createOrReplaceTempView("top_zipcodes_joined")


    # Υπολογισμός της συχνότητας εμφάνισης της κάθε τιμής στη στήλη "Descent", με φίλτρο για να αποκλείσουμε τις τιμές "Other" και "None"
    descent_counts = spark.sql("""
        SELECT Descent, COUNT(*) as count
        FROM top_zipcodes_joined
        WHERE  Descent != 'None'
        GROUP BY Descent
        ORDER BY count DESC
    """)

    # Εκτύπωση των αποτελεσμάτων
    descent_counts.show()

    # SQL query για να βρούμε τα 3 ZIP codes με το μικρότερο μέσο εισόδημα
    bottom_zipcodes = spark.sql("""
        SELECT `ZIPcode`, AVG(`Estimated Median Income`) as avg_income
        FROM final_table
        GROUP BY `ZIPcode`
        ORDER BY avg_income ASC
        LIMIT 3
    """)
    bottom_zipcodes.createOrReplaceTempView("bottom_zipcodes")

    # Join τον αρχικό πίνακα με τα τρία κορυφαία ZIP codes
    bottom_zipcodes_joined = spark.sql("""
        SELECT ft.*
        FROM final_table ft
        INNER JOIN bottom_zipcodes bz
        ON ft.ZIPcode = bz.ZIPcode
    """)
    bottom_zipcodes_joined.createOrReplaceTempView("bottom_zipcodes_joined")


    # Υπολογισμός της συχνότητας εμφάνισης της κάθε τιμής στη στήλη "Descent", με φίλτρο για να αποκλείσουμε τις τιμές "Other" και "None"
    descent_counts = spark.sql("""
        SELECT Descent, COUNT(*) as count
        FROM bottom_zipcodes_joined
        WHERE Descent NOT IN ('None')
        GROUP BY Descent
        ORDER BY count DESC
    """)

    # Εκτύπωση των αποτελεσμάτων
    descent_counts.show()

    spark.stop()


def use_suffle_hash():
    spark = SparkSession.builder \
        .appName("CrimeDataFilter-ShuffleHashJoin") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .getOrCreate()

    file_path = "hdfs://master:9000/home/user/exercise_datasets/LA_income_2015.csv"
    table1 = spark.read.csv(file_path, header=True, inferSchema=True)

    file_path = "hdfs://master:9000/home/user/exercise_datasets/Crime_Data_from_2010_to_2019.csv"
    crime_data1 = spark.read.csv(file_path, header=True, inferSchema=True)
    file_path = "hdfs://master:9000/home/user/exercise_datasets/Crime_Data_from_2020_to_Present.csv"
    crime_data2 = spark.read.csv(file_path, header=True, inferSchema=True)
    table2 = crime_data1.union(crime_data2)

    crime_2015 = table2.withColumn('Date Rptd', F.to_timestamp(F.col('Date Rptd'), 'MM/dd/yyyy hh:mm:ss a'))

    # Filter the records for the year 2015
    crime_2015 = crime_2015.filter(F.year(F.col('Date Rptd')) == 2015)



    selected_columns = ["Date Rptd","LAT", "LON", "Vict Descent"]
    table2 = crime_2015.select(*selected_columns)


    file_path = "hdfs://master:9000/home/user/exercise_datasets/revgecoding.csv"
    table3 = spark.read.csv(file_path, header=True, inferSchema=True)

    table2 = table2.withColumn(
        "Descent",
        F.expr("""
            CASE
                WHEN trim(`Vict Descent`) == "B" THEN 'Black'
                WHEN trim(`Vict Descent`) == "W" THEN 'White'
                WHEN trim(`Vict Descent`) == "A" THEN 'Other Asian'
                WHEN trim(`Vict Descent`) == "C" THEN 'Chinese'
                WHEN trim(`Vict Descent`) == "D" THEN 'Cambodian'
                WHEN trim(`Vict Descent`) == "F" THEN 'Filipino'
                WHEN trim(`Vict Descent`) == "G" THEN 'Guamanian'
                WHEN trim(`Vict Descent`) == "H" THEN 'Hispanic/Latin/Mexican'
                WHEN trim(`Vict Descent`) == "J" THEN 'Japanese'
                WHEN trim(`Vict Descent`) == "K" THEN 'Korean'
                WHEN trim(`Vict Descent`) == "L" THEN 'Laotian'
                WHEN trim(`Vict Descent`) == "P" THEN 'Pacific'
                WHEN trim(`Vict Descent`) == "S" THEN 'Samoan'
                WHEN trim(`Vict Descent`) == "U" THEN 'Hawaiian'
                WHEN trim(`Vict Descent`) == "V" THEN 'Vietnamese'
                WHEN trim(`Vict Descent`) == "Z" THEN 'Asian Indian'
                WHEN trim(`Vict Descent`) == "X" THEN 'Unknown'
                WHEN trim(`Vict Descent`) == "I" THEN 'American Indian/Alaskan Native'
                WHEN trim(`Vict Descent`) == "O" THEN 'Other'
                ELSE 'None'
            END
        """)
    )

    join_table = table3.hint("SHUFFLE_HASH").join(table2, (table3["LAT"] == table2["LAT"]) & (table3["LON"] == table2["LON"]), "inner")
    join_table.explain()
    final_table = table1.hint("SHUFFLE_HASH").join(join_table, table1["Zip Code"] == join_table["ZIPcode"], "inner")
    final_table.explain()


    # SQL query για να βρούμε τα 3 ZIP codes με το μεγαλύτερο μέσο εισόδημα
    final_table = final_table.withColumn(
        "Estimated Median Income",
        regexp_replace(col("Estimated Median Income"), "[$,]", "").cast("double")
    )
    final_table.createOrReplaceTempView("final_table")
    top_zipcodes = spark.sql("""
        SELECT ZIPcode, AVG(`Estimated Median Income`) as avg_income
        FROM final_table
        GROUP BY ZIPcode
        ORDER BY avg_income DESC
        LIMIT 3
    """)
    top_zipcodes.createOrReplaceTempView("top_zipcodes")


    # Join τον αρχικό πίνακα με τα τρία κορυφαία ZIP codes
    top_zipcodes_joined = spark.sql("""
        SELECT ft.*
        FROM final_table ft
        INNER JOIN top_zipcodes tz
        ON ft.ZIPcode = tz.ZIPcode
    """)
    top_zipcodes_joined.createOrReplaceTempView("top_zipcodes_joined")


    # Υπολογισμός της συχνότητας εμφάνισης της κάθε τιμής στη στήλη "Descent", με φίλτρο για να αποκλείσουμε τις τιμές "Other" και "None"
    descent_counts = spark.sql("""
        SELECT Descent, COUNT(*) as count
        FROM top_zipcodes_joined
        WHERE  Descent != 'None'
        GROUP BY Descent
        ORDER BY count DESC
    """)

    # Εκτύπωση των αποτελεσμάτων
    descent_counts.show()

    # SQL query για να βρούμε τα 3 ZIP codes με το μικρότερο μέσο εισόδημα
    bottom_zipcodes = spark.sql("""
        SELECT `ZIPcode`, AVG(`Estimated Median Income`) as avg_income
        FROM final_table
        GROUP BY `ZIPcode`
        ORDER BY avg_income ASC
        LIMIT 3
    """)
    bottom_zipcodes.createOrReplaceTempView("bottom_zipcodes")

    # Join τον αρχικό πίνακα με τα τρία κορυφαία ZIP codes
    bottom_zipcodes_joined = spark.sql("""
        SELECT ft.*
        FROM final_table ft
        INNER JOIN bottom_zipcodes bz
        ON ft.ZIPcode = bz.ZIPcode
    """)
    bottom_zipcodes_joined.createOrReplaceTempView("bottom_zipcodes_joined")


    # Υπολογισμός της συχνότητας εμφάνισης της κάθε τιμής στη στήλη "Descent", με φίλτρο για να αποκλείσουμε τις τιμές "Other" και "None"
    descent_counts = spark.sql("""
        SELECT Descent, COUNT(*) as count
        FROM bottom_zipcodes_joined
        WHERE Descent NOT IN ('None')
        GROUP BY Descent
        ORDER BY count DESC
    """)

    # Εκτύπωση των αποτελεσμάτων
    descent_counts.show()

    spark.stop()


def use_shuffle_replicate_nl():
    spark = SparkSession.builder \
        .appName("CrimeDataFilter") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .getOrCreate()

    file_path = "LA_income_2015.csv"
    table1 = spark.read.csv(file_path, header=True, inferSchema=True)

    file_path = "Crime_Data_from_2010_to_2019.csv"
    crime_data1 = spark.read.csv(file_path, header=True, inferSchema=True)
    file_path = "Crime_Data_from_2020_to_Present.csv"
    crime_data2 = spark.read.csv(file_path, header=True, inferSchema=True)
    table2 = crime_data1.union(crime_data2)

    crime_2015 = table2.withColumn('Date Rptd', F.to_timestamp(F.col('Date Rptd'), 'MM/dd/yyyy hh:mm:ss a'))

    # Filter the records for the year 2015
    crime_2015 = crime_2015.filter(F.year(F.col('Date Rptd')) == 2015)

    selected_columns = ["Date Rptd", "LAT", "LON", "Vict Descent"]
    table2 = crime_2015.select(*selected_columns)

    file_path = "revgecoding.csv"
    table3 = spark.read.csv(file_path, header=True, inferSchema=True)

    table2 = table2.withColumn(
        "Descent",
        F.expr("""
             CASE
                 WHEN trim(`Vict Descent`) == "B" THEN 'Black'
                 WHEN trim(`Vict Descent`) == "W" THEN 'White'
                 WHEN trim(`Vict Descent`) == "A" THEN 'Other Asian'
                 WHEN trim(`Vict Descent`) == "C" THEN 'Chinese'
                 WHEN trim(`Vict Descent`) == "D" THEN 'Cambodian'
                 WHEN trim(`Vict Descent`) == "F" THEN 'Filipino'
                 WHEN trim(`Vict Descent`) == "G" THEN 'Guamanian'
                 WHEN trim(`Vict Descent`) == "H" THEN 'Hispanic/Latin/Mexican'
                 WHEN trim(`Vict Descent`) == "J" THEN 'Japanese'
                 WHEN trim(`Vict Descent`) == "K" THEN 'Korean'
                 WHEN trim(`Vict Descent`) == "L" THEN 'Laotian'
                 WHEN trim(`Vict Descent`) == "P" THEN 'Pacific'
                 WHEN trim(`Vict Descent`) == "S" THEN 'Samoan'
                 WHEN trim(`Vict Descent`) == "U" THEN 'Hawaiian'
                 WHEN trim(`Vict Descent`) == "V" THEN 'Vietnamese'
                 WHEN trim(`Vict Descent`) == "Z" THEN 'Asian Indian'
                 WHEN trim(`Vict Descent`) == "X" THEN 'Unknown'
                 WHEN trim(`Vict Descent`) == "I" THEN 'American Indian/Alaskan Native'
                 WHEN trim(`Vict Descent`) == "O" THEN 'Other'
                 ELSE 'None'
             END
         """)
    )

    join_table = table3.hint("SHUFFLE_REPLICATE_NL").join(table2, (table3["LAT"] == table2["LAT"]) & (table3["LON"] == table2["LON"]), "inner")
    join_table.explain()
    final_table = table1.hint("SHUFFLE_REPLICATE_NL").join(join_table, table1["Zip Code"] == join_table["ZIPcode"], "inner")
    final_table.explain()


    # SQL query για να βρούμε τα 3 ZIP codes με το μεγαλύτερο μέσο εισόδημα
    final_table = final_table.withColumn(
        "Estimated Median Income",
        regexp_replace(col("Estimated Median Income"), "[$,]", "").cast("double")
    )
    final_table.createOrReplaceTempView("final_table")
    top_zipcodes = spark.sql("""
        SELECT ZIPcode, AVG(`Estimated Median Income`) as avg_income
        FROM final_table
        GROUP BY ZIPcode
        ORDER BY avg_income DESC
        LIMIT 3
    """)
    top_zipcodes.createOrReplaceTempView("top_zipcodes")


    # Join τον αρχικό πίνακα με τα τρία κορυφαία ZIP codes
    top_zipcodes_joined = spark.sql("""
        SELECT ft.*
        FROM final_table ft
        INNER JOIN top_zipcodes tz
        ON ft.ZIPcode = tz.ZIPcode
    """)
    top_zipcodes_joined.createOrReplaceTempView("top_zipcodes_joined")


    # Υπολογισμός της συχνότητας εμφάνισης της κάθε τιμής στη στήλη "Descent", με φίλτρο για να αποκλείσουμε τις τιμές "Other" και "None"
    descent_counts = spark.sql("""
        SELECT Descent, COUNT(*) as count
        FROM top_zipcodes_joined
        WHERE  Descent != 'None'
        GROUP BY Descent
        ORDER BY count DESC
    """)

    # Εκτύπωση των αποτελεσμάτων
    descent_counts.show()

    # SQL query για να βρούμε τα 3 ZIP codes με το μικρότερο μέσο εισόδημα
    bottom_zipcodes = spark.sql("""
        SELECT `ZIPcode`, AVG(`Estimated Median Income`) as avg_income
        FROM final_table
        GROUP BY `ZIPcode`
        ORDER BY avg_income ASC
        LIMIT 3
    """)
    bottom_zipcodes.createOrReplaceTempView("bottom_zipcodes")

    # Join τον αρχικό πίνακα με τα τρία κορυφαία ZIP codes
    bottom_zipcodes_joined = spark.sql("""
        SELECT ft.*
        FROM final_table ft
        INNER JOIN bottom_zipcodes bz
        ON ft.ZIPcode = bz.ZIPcode
    """)
    bottom_zipcodes_joined.createOrReplaceTempView("bottom_zipcodes_joined")


    # Υπολογισμός της συχνότητας εμφάνισης της κάθε τιμής στη στήλη "Descent", με φίλτρο για να αποκλείσουμε τις τιμές "Other" και "None"
    descent_counts = spark.sql("""
        SELECT Descent, COUNT(*) as count
        FROM bottom_zipcodes_joined
        WHERE Descent NOT IN ('None')
        GROUP BY Descent
        ORDER BY count DESC
    """)

    # Εκτύπωση των αποτελεσμάτων
    descent_counts.show()

    spark.stop()


def use_merge():
    spark = SparkSession.builder \
        .appName("CrimeDataFilter") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .getOrCreate()

    print("\n\n")
    print("Execute with shuffle hash join")
    start_time = time.time()

    file_path = "LA_income_2015.csv"
    table1 = spark.read.csv(file_path, header=True, inferSchema=True)

    file_path = "Crime_Data_from_2010_to_2019.csv"
    crime_data1 = spark.read.csv(file_path, header=True, inferSchema=True)
    file_path = "Crime_Data_from_2020_to_Present.csv"
    crime_data2 = spark.read.csv(file_path, header=True, inferSchema=True)
    table2 = crime_data1.union(crime_data2)

    crime_2015 = table2.withColumn('Date Rptd', F.to_timestamp(F.col('Date Rptd'), 'MM/dd/yyyy hh:mm:ss a'))

    # Filter the records for the year 2015
    crime_2015 = crime_2015.filter(F.year(F.col('Date Rptd')) == 2015)



    selected_columns = ["Date Rptd","LAT", "LON", "Vict Descent"]
    table2 = crime_2015.select(*selected_columns)


    file_path = "revgecoding.csv"
    table3 = spark.read.csv(file_path, header=True, inferSchema=True)

    read_csv_time = time.time()
    print(f"Time to read CSV: {read_csv_time - start_time:.2f} seconds")

    query_start_time = time.time()

    table2 = table2.withColumn(
        "Descent",
        F.expr("""
            CASE
                WHEN trim(`Vict Descent`) == "B" THEN 'Black'
                WHEN trim(`Vict Descent`) == "W" THEN 'White'
                WHEN trim(`Vict Descent`) == "A" THEN 'Other Asian'
                WHEN trim(`Vict Descent`) == "C" THEN 'Chinese'
                WHEN trim(`Vict Descent`) == "D" THEN 'Cambodian'
                WHEN trim(`Vict Descent`) == "F" THEN 'Filipino'
                WHEN trim(`Vict Descent`) == "G" THEN 'Guamanian'
                WHEN trim(`Vict Descent`) == "H" THEN 'Hispanic/Latin/Mexican'
                WHEN trim(`Vict Descent`) == "J" THEN 'Japanese'
                WHEN trim(`Vict Descent`) == "K" THEN 'Korean'
                WHEN trim(`Vict Descent`) == "L" THEN 'Laotian'
                WHEN trim(`Vict Descent`) == "P" THEN 'Pacific'
                WHEN trim(`Vict Descent`) == "S" THEN 'Samoan'
                WHEN trim(`Vict Descent`) == "U" THEN 'Hawaiian'
                WHEN trim(`Vict Descent`) == "V" THEN 'Vietnamese'
                WHEN trim(`Vict Descent`) == "Z" THEN 'Asian Indian'
                WHEN trim(`Vict Descent`) == "X" THEN 'Unknown'
                WHEN trim(`Vict Descent`) == "I" THEN 'American Indian/Alaskan Native'
                WHEN trim(`Vict Descent`) == "O" THEN 'Other'
                ELSE 'None'
            END
        """)
    )

    join_table = pd.merge(table1, table2, on=["LAT", "LON"], how="inner")
    final_table = pd.merge(join_table, table3, left_on="Zip Code", right_on="ZIPcode", how="inner")
    join_table.explain()
    final_table.explain()
    join_time=time.time()
    print(f"It took {join_time-query_start_time } seconds to execute join")


    # SQL query για να βρούμε τα 3 ZIP codes με το μεγαλύτερο μέσο εισόδημα
    final_table = final_table.withColumn(
        "Estimated Median Income",
        regexp_replace(col("Estimated Median Income"), "[$,]", "").cast("double")
    )
    final_table.createOrReplaceTempView("final_table")
    top_zipcodes = spark.sql("""
        SELECT ZIPcode, AVG(`Estimated Median Income`) as avg_income
        FROM final_table
        GROUP BY ZIPcode
        ORDER BY avg_income DESC
        LIMIT 3
    """)
    top_zipcodes.createOrReplaceTempView("top_zipcodes")


    # Join τον αρχικό πίνακα με τα τρία κορυφαία ZIP codes
    top_zipcodes_joined = spark.sql("""
        SELECT ft.*
        FROM final_table ft
        INNER JOIN top_zipcodes tz
        ON ft.ZIPcode = tz.ZIPcode
    """)
    top_zipcodes_joined.createOrReplaceTempView("top_zipcodes_joined")


    # Υπολογισμός της συχνότητας εμφάνισης της κάθε τιμής στη στήλη "Descent", με φίλτρο για να αποκλείσουμε τις τιμές "Other" και "None"
    descent_counts = spark.sql("""
        SELECT Descent, COUNT(*) as count
        FROM top_zipcodes_joined
        WHERE  Descent != 'None'
        GROUP BY Descent
        ORDER BY count DESC
    """)

    # Εκτύπωση των αποτελεσμάτων
    descent_counts.show()

    # SQL query για να βρούμε τα 3 ZIP codes με το μικρότερο μέσο εισόδημα
    bottom_zipcodes = spark.sql("""
        SELECT `ZIPcode`, AVG(`Estimated Median Income`) as avg_income
        FROM final_table
        GROUP BY `ZIPcode`
        ORDER BY avg_income ASC
        LIMIT 3
    """)
    bottom_zipcodes.createOrReplaceTempView("bottom_zipcodes")

    # Join τον αρχικό πίνακα με τα τρία κορυφαία ZIP codes
    bottom_zipcodes_joined = spark.sql("""
        SELECT ft.*
        FROM final_table ft
        INNER JOIN bottom_zipcodes bz
        ON ft.ZIPcode = bz.ZIPcode
    """)
    bottom_zipcodes_joined.createOrReplaceTempView("bottom_zipcodes_joined")


    # Υπολογισμός της συχνότητας εμφάνισης της κάθε τιμής στη στήλη "Descent", με φίλτρο για να αποκλείσουμε τις τιμές "Other" και "None"
    descent_counts = spark.sql("""
        SELECT Descent, COUNT(*) as count
        FROM bottom_zipcodes_joined
        WHERE Descent NOT IN ('None')
        GROUP BY Descent
        ORDER BY count DESC
    """)

    # Εκτύπωση των αποτελεσμάτων
    descent_counts.show()

    print("\n")
    query_end_time = time.time()
    print(f"Time to execute query: {query_end_time - query_start_time:.2f} seconds")
    show_time = time.time()
    print(f"Time to show results: {show_time - query_end_time:.2f} seconds")

    total_time = show_time - start_time
    print(f"Total execution time: {total_time:.2f} seconds")

    spark.stop()
	

print("Execute with simple join")
use_join()

print("Execute with broadcast join")
use_broadcast()

print("Execute with shuffle hash join")
use_suffle_hash()

# Problem with pandas installation - kept as an algorithm
# print("Execute with merge")
# use_merge()

# Too much time needed to execute - kept as an algorithm
# print("Execute with shuffle replicate")
# use_shuffle_replicate_nl()