# Overview
This repository contains the coursework for the Big Data Processing course, focusing on analyzing the New York 'Uber/Lyft' rideshare data from January 1, 2023, to May 31, 2023. The dataset, pre-processed by the NYC Taxi and Limousine Commission (TLC), is used to apply Spark techniques for various analytical tasks.

# Dataset
The dataset is provided under the path //data-repository-bkt/ECS765/rideshare_2023/ and includes:
- rideshare_data.csv
- taxi_zone_lookup.csv

# Assignment Tasks
 ## Task 1: Merging Datasets (15 points)
 - Load and join the datasets based on location fields.
 - Convert the date field from UNIX timestamp to yyyy-MM-dd format.
 - Print the number of rows and schema of the new dataframe.
 - <img width="564" alt="截圖 2025-02-17 00 13 55" src="https://github.com/user-attachments/assets/50f1e9e2-bb5b-43cb-bbe9-5569059ce496" />

## Task 2: Aggregation of Data (20 points)
 - Count trips per business per month.
 - Calculate platform profits per business per month.
 - Calculate driver earnings per business per month.
 - Provide insights from the results.
 - ![Uploading 截圖 2025-02-17 00.14.29.png…]()
## Task 3
: Top-K Processing
- Identify the top 5 popular pickup and dropoff boroughs each month.
- Identify the top 30 earning routes.
- 
##Task 4: Average of Data
- Calculate the average driver pay and trip length for different times of the day.
- Calculate average earnings per mile for different times of the day.
## Task 5: Finding Anomalies
- Calculate average waiting time in January.
- Identify and analyze days with waiting time exceeding 300 seconds.
- <img width="539" alt="截圖 2025-02-17 00 15 08" src="https://github.com/user-attachments/assets/bca3dd62-9626-4150-bc52-0b9bc56d860a" />

## Task 6: Filtering Data
- Find trip counts within a specified range for different pickup boroughs and times of day.
- Calculate trips in the evening time and trips from Brooklyn to Staten Island.
## Task 7: Routes Analysis
- Analyze the top 10 popular routes based on trip count.
- Optional
## Task 8: Graph Processing
- Define vertex and edge schemas and construct dataframes.

# License
The dataset is distributed under the MIT license.
