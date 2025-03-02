# Module 5 Homework

In this homework we'll put what we learned about Spark in practice.

For this homework we will be using the Yellow 2024-10 data from the official website: 

```bash
wget https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-10.parquet
```


## Question 1: Install Spark and PySpark

- Install Spark
- Run PySpark
- Create a local spark session
- Execute spark.version.

What's the output?

```3.3.5 ```
> [!NOTE]
> To install PySpark follow this [guide](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/05-batch/setup/pyspark.md)


## Question 2: Yellow October 2024

Read the October 2024 Yellow into a Spark Dataframe.

Repartition the Dataframe to 4 partitions and save it to parquet.

What is the average size of the Parquet (ending with .parquet extension) Files that were created (in MB)? Select the answer which most closely matches.

- 6MB
- 25MB
- 75MB
- 100MB

```python
df = spark.read.parquet("yellow_tripdata_2024-10.parquet")
df_repartitioned = df.repartition(4)
output_path = "output_parquet"
df_repartitioned.write.mode("overwrite").parquet(output_path)
parquet_files = [f for f in os.listdir(output_path) if f.endswith(".parquet")]
total_size = sum(os.path.getsize(os.path.join(output_path, f)) for f in parquet_files)
avg_size_mb = (total_size / len(parquet_files)) / (1024 * 1024)

print(f"Average Parquet File Size: {avg_size_mb:.2f} MB")
#Average Parquet File Size: 22.40 MB

```



## Question 3: Count records 

How many taxi trips were there on the 15th of October?

Consider only trips that started on the 15th of October.

- 85,567
- 105,567
- 125,567
- 145,567
```python 
df.createOrReplaceTempView('trips_data')

spark.sql("""
    SELECT COUNT(*) 
    FROM trips_data 
    WHERE TO_DATE(tpep_pickup_datetime) = '2024-10-15' 
""").show()
#Number of taxi trips that started and ended on October 15, 2024: 128893
```


## Question 4: Longest trip

What is the length of the longest trip in the dataset in hours?

- 122
- 142
- 162
- 182

```python
spark.sql("""
    SELECT 
        MAX(TIMESTAMPDIFF(SECOND, tpep_pickup_datetime, tpep_dropoff_datetime)) / 3600 AS longest_trip_hours
    FROM trips_data
""").show()
#output
+------------------+
|longest_trip_hours|
+------------------+
|162.61777777777777|
+------------------+
```
## Question 5: User Interface

Spark’s User Interface which shows the application's dashboard runs on which local port?

- 80
- 443

```- 4040```
- 8080



## Question 6: Least frequent pickup location zone

Load the zone lookup data into a temp view in Spark:

```bash
wget https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv
```

Using the zone lookup data and the Yellow October 2024 data, what is the name of the LEAST frequent pickup location Zone?

- Governor's Island/Ellis Island/Liberty Island
- Arden Heights
- Rikers Island
- Jamaica Bay

```python
result = spark.sql("""
    SELECT z.Zone, COUNT(*) AS trip_count
    FROM taxi_data t
    JOIN zone_data z
    ON t.PULocationID = z.LocationID
    GROUP BY z.Zone
    ORDER BY trip_count ASC
   
""")

result.show()
#output
+--------------------+----------+
|                Zone|trip_count|
+--------------------+----------+
|Governor's Island...|         1|
|       Rikers Island|         2|
|       Arden Heights|         2|
|         Jamaica Bay|         3|
| Green-Wood Cemetery|         3|
|Charleston/Totten...|         4|
|   Rossville/Woodrow|         4|
|       West Brighton|         4|
|Eltingville/Annad...|         4|
|       Port Richmond|         4|
|         Great Kills|         6|
|        Crotona Park|         6|
|Heartland Village...|         7|
|     Mariners Harbor|         7|
|Saint George/New ...|         9|
|             Oakwood|         9|
|       Broad Channel|        10|
|New Dorp/Midland ...|        10|
|         Westerleigh|        12|
|     Pelham Bay Park|        12|
+--------------------+----------+
```
## Submitting the solutions

- Form for submitting: https://courses.datatalks.club/de-zoomcamp-2025/homework/hw5
- Deadline: See the website
