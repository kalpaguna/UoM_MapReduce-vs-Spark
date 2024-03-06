-- analyze_delays.hql

-- Drop the existing table if it exists
DROP TABLE IF EXISTS delay_flights;

-- Create a table to store the airlines dataset
CREATE EXTERNAL TABLE delay_flights (
    Year INT,
    CarrierDelay INT,
    NASDelay INT,
    WeatherDelay INT,
    LateAircraftDelay INT,
    SecurityDelay INT,
    ArrDelay INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION 's3://flight-delay-data-bucket/delay-data/';


-- Run analysis queries for carrier delay, NAS delay, weather delay, late aircraft delay, and security delay
SELECT Year, AVG(CarrierDelay / ArrDelay * 100) AS avg_carrier_delay
FROM delay_flights
WHERE Year BETWEEN 2003 AND 2010
GROUP BY Year;

SELECT Year, AVG(NASDelay / ArrDelay * 100) AS avg_nas_delay
FROM delay_flights
WHERE Year BETWEEN 2003 AND 2010
GROUP BY Year;

SELECT Year, AVG(WeatherDelay / ArrDelay * 100) AS avg_weather_delay
FROM delay_flights
WHERE Year BETWEEN 2003 AND 2010
GROUP BY Year;

SELECT Year, AVG(LateAircraftDelay / ArrDelay * 100) AS avg_late_aircraft_delay
FROM delay_flights
WHERE Year BETWEEN 2003 AND 2010
GROUP BY Year;

SELECT Year, AVG(SecurityDelay / ArrDelay * 100) AS avg_security_delay
FROM delay_flights
WHERE Year BETWEEN 2003 AND 2010
GROUP BY Year;

