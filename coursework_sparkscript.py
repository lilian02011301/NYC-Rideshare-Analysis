import sys, string
import os
import socket
import time
import operator
import boto3
import json
from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql.functions import from_unixtime, date_format
from pyspark.sql.functions import to_date, count, col
from graphframes import *
from pyspark.sql.functions import month, avg, sum
from pyspark.sql.functions import rank, lit, concat_ws 
from pyspark.sql.window import Window


if __name__ == "__main__":

    spark = SparkSession\
        .builder\
        .appName("TestDataset")\
        .getOrCreate()
    
    # shared read-only object bucket containing datasets
    s3_data_repository_bucket = os.environ['DATA_REPOSITORY_BUCKET']

    s3_endpoint_url = os.environ['S3_ENDPOINT_URL']+':'+os.environ['BUCKET_PORT']
    s3_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
    s3_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
    s3_bucket = os.environ['BUCKET_NAME']

    hadoopConf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoopConf.set("fs.s3a.endpoint", s3_endpoint_url)
    hadoopConf.set("fs.s3a.access.key", s3_access_key_id)
    hadoopConf.set("fs.s3a.secret.key", s3_secret_access_key)
    hadoopConf.set("fs.s3a.path.style.access", "true")
    hadoopConf.set("fs.s3a.connection.ssl.enabled", "false")

#task1_1
    rideshare_data = spark.read.csv("s3a://" + s3_data_repository_bucket + "/ECS765/rideshare_2023/rideshare_data.csv", header = True)
    taxi_zone_lookup = spark.read.csv("s3a://" + s3_data_repository_bucket + "/ECS765/rideshare_2023/taxi_zone_lookup.csv", header = True)


#task1_2
# First, join with taxi_zone_lookup on pickup_location
    pickup_joined = rideshare_data.join(
    taxi_zone_lookup,
    rideshare_data.pickup_location == taxi_zone_lookup.LocationID,
    'left'
).select(
    rideshare_data["*"],  # Preserve original columns from rideshare_data
    col("Borough").alias("Pickup_Borough"),
    col("Zone").alias("Pickup_Zone"),
    col("service_zone").alias("Pickup_service_zone")
)

# Next, join the result with taxi_zone_lookup on dropoff_location
    final_df = pickup_joined.join(
    taxi_zone_lookup,
    pickup_joined.dropoff_location == taxi_zone_lookup.LocationID,
    'left'
).select(
    pickup_joined["*"],  # Preserve all columns from the intermediate DataFrame
    col("Borough").alias("Dropoff_Borough"),
    col("Zone").alias("Dropoff_Zone"),
    col("service_zone").alias("Dropoff_service_zone")
)

# Show some results to verify the outcome
    final_df.show()

#task1_3
# Convert the UNIX timestamp in the 'date' field to 'yyyy-MM-dd' format
    final_df_with_converted_date = final_df.withColumn(
    "date",
    to_date(from_unixtime(col("date")), 'yyyy-MM-dd')
)


# Now, the 'date' column will be in the desired format. You can show some rows to verify.
    final_df_with_converted_date.show()

#task1_4
    row_count = final_df_with_converted_date.count()
    print(f"Number of rows: {row_count}")

    final_df_with_converted_date.printSchema()

#task2_1
    df_with_month = final_df_with_converted_date.withColumn("month", month("date"))
    df_with_month = df_with_month.withColumn("business_month", concat_ws("-", col("business"), col("month")))
# Count trips per business-month
    trips_count = df_with_month.groupBy("business_month").count().orderBy("business_month")
    trips_count.show()

# Save the aggregated data to a CSV file
#now = datetime.now() 
#date_time = now.strftime("%d-%m-%Y_%H:%M:%S")

#my_bucket_resource = boto3.resource('s3',
            #endpoint_url='http://' + s3_endpoint_url,
            #aws_access_key_id=s3_access_key_id,
            #aws_secret_access_key=s3_secret_access_key)
    
#csv_trips_count = trips_count_sorted.toPandas().to_csv(index = False, header = True)
#my_result_object = my_bucket_resource.Object(s3_bucket,'coursework' + date_time + '/trips_count.csv')
#my_result_object = my_result_object.put(Body= csv_trips_count)



#task2_2
    #Calculate the platform's profits per business-month
    df_with_month = df_with_month.withColumn("rideshare_profit", col("rideshare_profit").cast("float"))
    profits = df_with_month.groupBy("business_month").sum("rideshare_profit").orderBy("business_month")
    profits.show()

# Save the aggregated data to a CSV file
#now = datetime.now() 
#date_time = now.strftime("%d-%m-%Y_%H:%M:%S")

#my_bucket_resource = boto3.resource('s3',
            #endpoint_url='http://' + s3_endpoint_url,
            #aws_access_key_id=s3_access_key_id,
            #aws_secret_access_key=s3_secret_access_key)
    
#csv_profits = profits_sorted.toPandas().to_csv(index = False, header = True)
#my_result_object = my_bucket_resource.Object(s3_bucket,'coursework' + date_time + '/profits.csv')
#my_result_object = my_result_object.put(Body= csv_profits)    


#task2_3
# Calculate the driver's earnings per business-month
    df_with_month = df_with_month.withColumn("driver_total_pay", col("driver_total_pay").cast("float"))
    earnings = df_with_month.groupBy("business_month").sum("driver_total_pay").orderBy("business_month")
    earnings.show()

# Save the aggregated data to a CSV file
#now = datetime.now() 
#date_time = now.strftime("%d-%m-%Y_%H:%M:%S")

#my_bucket_resource = boto3.resource('s3',
            #endpoint_url='http://' + s3_endpoint_url,
            #aws_access_key_id=s3_access_key_id,
            #aws_secret_access_key=s3_secret_access_key)
    
#csv_earnings = earnings_sorted.toPandas().to_csv(index = False, header = True)
#my_result_object = my_bucket_resource.Object(s3_bucket,'coursework' + date_time + '/earnings.csv')
#my_result_object = my_result_object.put(Body= csv_earnings)    


#task3_1
    df_with_month = final_df_with_converted_date.withColumn("Month", month(col("date")))
# Count trips by Pickup_Borough and Month
    borough_month_trip_counts = df_with_month.groupBy("Pickup_Borough", "Month").count().withColumnRenamed("count", "trip_count")
    windowSpec = Window.partitionBy("Month").orderBy(col("trip_count").desc())

# Rank boroughs within each month
    ranked_boroughs = borough_month_trip_counts.withColumn("rank", rank().over(windowSpec))
    top_5_boroughs_each_month = ranked_boroughs.filter(col("rank") <= 5)
    final_output = top_5_boroughs_each_month.select("Pickup_Borough", "Month", "trip_count").orderBy("Month", col("trip_count").desc())
    final_output.show()

#task3_2
    dropoff_borough_trip_counts = df_with_month.groupBy("Dropoff_Borough", "Month").count().withColumnRenamed("count", "trip_count")
    
# Window specification to partition by month and order by trip_count in descending order
    windowSpec = Window.partitionBy("Month").orderBy(col("trip_count").desc())
# Rank boroughs within each month
    ranked_boroughs = dropoff_borough_trip_counts.withColumn("rank", rank().over(windowSpec))
    top_5_dropoff_boroughs_each_month = ranked_boroughs.filter(col("rank") <= 5)
    final_dropoff_output = top_5_dropoff_boroughs_each_month.select("Dropoff_Borough", "Month", "trip_count").orderBy("Month", col("trip_count").desc())
    final_dropoff_output.show(50, truncate=False)

#task3_3
# Assuming 'Pickup_Borough' and 'Dropoff_Borough' are the correct column names in your DataFrame
    df_with_route = final_df_with_converted_date.withColumn(
    "Route",concat_ws(" to ", col("Pickup_Borough"), col("Dropoff_Borough")))

# Ensure 'driver_total_pay' is a float to allow for summation
    df_with_route = df_with_route.withColumn("driver_total_pay", col("driver_total_pay").cast("float"))
    
# Group by 'Route' and sum 'driver_total_pay' for each route to get 'total_profit'
    route_profits = df_with_route.groupBy("Route").agg(sum("driver_total_pay").alias("total_profit"))
    top_30_routes = route_profits.orderBy(col("total_profit").desc()).limit(30)
    top_30_routes.show(30, truncate=False)



#task4_1
# Average 'driver_total_pay' by 'time_of_day'
avg_driver_pay = df_with_month.groupBy("time_of_day").agg(avg("driver_total_pay").alias("average_driver_total_pay"))
avg_driver_pay = avg_driver_pay.orderBy(col("average_driver_total_pay").desc())
avg_driver_pay.show()

#task4_2
# Average 'trip_length' by 'time_of_day'
avg_trip_length = df_with_month.groupBy("time_of_day").agg(avg("trip_length").alias("average_trip_length"))
avg_trip_length = avg_trip_length.orderBy(col("average_trip_length").desc())
avg_trip_length.show()

#task4_3
#Calculate Average Earned per Mile
combined = avg_driver_pay.join(avg_trip_length, "time_of_day")
combined = combined.withColumn("average_earning_per_mile", col("average_driver_total_pay") / col("average_trip_length"))
combined = combined.select("time_of_day", "average_earning_per_mile").orderBy(col("average_earning_per_mile").desc())
combined.show()


#task5_1
january_data = final_df_with_converted_date.filter(month(col("date")) == 1)

# Check for the count of records per day to ensure all days are present
january_data.groupBy(dayofmonth("date").alias("day")).count().orderBy("day")

# Calculate average waiting time by day in January
avg_waiting_time_by_day = january_data.groupBy(dayofmonth("date").alias("day")) \
    .agg(avg("request_to_pickup").alias("average_waiting_time"))

# Sort the results by day and display them
avg_waiting_time_by_day_sorted = avg_waiting_time_by_day.orderBy("day")
avg_waiting_time_by_day_sorted.show(31)

# Save the aggregated data to a CSV file
#now = datetime.now() 
#date_time = now.strftime("%d-%m-%Y_%H:%M:%S")

#my_bucket_resource = boto3.resource('s3',
            #endpoint_url='http://' + s3_endpoint_url,
            #aws_access_key_id=s3_access_key_id,
            #aws_secret_access_key=s3_secret_access_key)
    
#csv_avg_waiting_time = avg_waiting_time_by_day_sorted.toPandas().to_csv(index = False, header = True)
#my_result_object = my_bucket_resource.Object(s3_bucket,'coursework' + date_time + '/avg_waiting_time.csv')
#my_result_object = my_result_object.put(Body= csv_avg_waiting_time)



#task6_1
#Find Trip Counts for Different 'Pickup_Borough' at Different 'Time_of_day'
trip_counts_by_borough_time = df_with_month.groupBy("Pickup_Borough", "time_of_day").agg(count("*").alias("trip_count"))

# filter the aggregated DataFrame for trip counts greater than 0 and less than 1000
trip_counts_filtered = trip_counts_by_borough_time.filter((col("trip_count") > 0) & (col("trip_count") < 1000))
trip_counts_filtered.show(truncate=False)


#task6_2
# Filter for trips in the evening time_of_day
evening_trips = df_with_month.filter(col("time_of_day") == "evening")

# Group by Pickup_Borough, then count
evening_trip_counts = evening_trips.groupBy("Pickup_Borough").agg(count("*").alias("trip_count"))

# Add a static column for 'time_of_day' to match the expected output
evening_trip_counts = evening_trip_counts.withColumn("time_of_day", lit("evening")).select("Pickup_Borough", "time_of_day", "trip_count")
evening_trip_counts.show(truncate=False)

#task6_3
# Filter for trips starting in Brooklyn and ending in Staten Island
brooklyn_to_staten_island_trips = df_with_month.filter((col("Pickup_Borough") == "Brooklyn") & (col("Dropoff_Borough") == "Staten Island"))

# Count such trips
num_trips_brooklyn_to_staten_island = brooklyn_to_staten_island_trips.count()
brooklyn_to_staten_island_trips.select("Pickup_Borough", "Dropoff_Borough", "Pickup_Zone").show(10, truncate=False)
print(f"Number of trips from Brooklyn to Staten Island: {num_trips_brooklyn_to_staten_island}")


#task 7 
df_with_routes = df_with_month.withColumn("Route", concat_ws(" to ", "Pickup_Zone", "Dropoff_Zone"))

route_counts = df_with_routes.groupBy("Route", "business").count()
route_counts_pivoted = route_counts.groupBy("Route").pivot("business").sum("count").fillna(0)
route_counts_final = route_counts_pivoted.withColumn("total_count", col("Uber") + col("Lyft")) \
    .withColumnRenamed("Uber", "uber_count") \
    .withColumnRenamed("Lyft", "lyft_count")

top_10_routes = route_counts_final.orderBy(col("total_count").desc()).limit(10)
top_10_routes.show(truncate=False)
   


spark.stop()