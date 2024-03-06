from pyspark.sql import SparkSession
import time
import csv
from pyspark.sql import Row

# Create a Spark session
spark = SparkSession.builder.appName("FlightDelayAnalysis").getOrCreate()

sc = spark.sparkContext
sc.setLogLevel('ERROR')

# Load data into a DataFrame
df = spark.read.csv('s3://flight-delay-data-bucket/delay-data/DelayedFlights-updated.csv', header=True, inferSchema=True)
df.createOrReplaceTempView("delay_flights")

# Define Spark-SQL queries
sql_queries = [
    "SELECT Year, AVG(CarrierDelay / ArrDelay * 100) AS avg_carrier_delay_percentage FROM delay_flights GROUP BY Year",
    "SELECT Year, AVG(NASDelay / ArrDelay * 100) AS avg_nas_delay_percentage FROM delay_flights GROUP BY Year",
    "SELECT Year, AVG(WeatherDelay / ArrDelay * 100) AS avg_weather_delay_percentage FROM delay_flights GROUP BY Year",
    "SELECT Year, AVG(LateAircraftDelay / ArrDelay * 100) AS avg_late_aircraft_delay_percentage FROM delay_flights GROUP BY Year",
    "SELECT Year, AVG(SecurityDelay / ArrDelay * 100) AS avg_security_delay_percentage FROM delay_flights GROUP BY Year"
]

# Number of iterations
num_iterations = 5

# Dictionary to store iteration number and corresponding execution times for each query
query_execution_times = {query: [] for query in sql_queries}

# Run queries for the specified number of iterations
for i in range(num_iterations):
    for query in sql_queries:
        start_time = time.time()
        result = spark.sql(query)
        execution_time = time.time() - start_time

        # Append data for plotting
        query_execution_times[query].append(execution_time)

        # Show the result 
        result.show()

header_labels = ['Carrier_delay','NAS_delay','Weather_delay','Late_aircraft_delay','Security_delay']
i = 0
        
csv_file_path = 'spark_execution_times.csv'

with open(csv_file_path, 'w', newline='') as csvfile:
    csv_writer = csv.writer(csvfile)
    
    # Write header
    csv_writer.writerow(['Query', 'Iteration', 'Execution Time (seconds)'])
    
    # Write data
    for query, execution_times in query_execution_times.items():
        for iteration, execution_time in enumerate(execution_times, start=1):
            csv_writer.writerow([header_labels[i], iteration, execution_time])
        i += 1