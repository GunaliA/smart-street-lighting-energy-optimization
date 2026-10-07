CREATE DATABASE IF NOT EXISTS smart_street_lighting;
USE smart_street_lighting;

DROP TABLE IF EXISTS street_light_data;

CREATE EXTERNAL TABLE street_light_data (
    event_timestamp TIMESTAMP,
    light_id STRING,
    location STRING,
    latitude DOUBLE,
    longitude DOUBLE,
    ldr_value INT,
    motion_detected INT,
    traffic_level STRING,
    brightness_level INT,
    voltage DOUBLE,
    current_value DOUBLE,
    power_consumption DOUBLE,
    temperature DOUBLE,
    light_status STRING,
    fault_status STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'hdfs://localhost:9000/smart-street-lighting/historical'
TBLPROPERTIES ('skip.header.line.count'='1');
