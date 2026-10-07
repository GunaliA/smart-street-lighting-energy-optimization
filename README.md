# Smart Street Lighting Energy Optimization System

Software-only Big Data project for simulating, ingesting, processing, analyzing, optimizing, and visualizing smart street-lighting data on Windows.

## Architecture

Python Simulator → Kafka → HDFS → Spark → Hive → Grafana

Kafka messages are JSON. The historical HDFS dataset is CSV.

## Important project definitions

- 10,000 physical lights are simulated.
- There are 10 zones with exactly 1,000 physical lights per zone.
- A `light_id` permanently belongs to one zone and one coordinate.
- 100,000 observation records are generated.
- `power_consumption` is watts (W).
- The simulator declares a 1-second observation interval, so `energy_kwh = power_w × 1 / 3,600,000`.
- Active/Faulty light KPIs use the latest observation for each physical light in the selected dataset.

## Prerequisites

Install separately on Windows:

- Java 8 JDK
- Hadoop 3.3.1 with Windows native binaries
- Apache Spark 3.3.4 (Hadoop 3)
- Apache Kafka 3.9.2
- Apache Hive 3.1.3
- Grafana OSS + Infinity datasource
- Python 3.11

The project does **not** bundle those large installations.

## Python environment

```powershell
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

## 1. Generate and validate data

```powershell
python src\simulator\simulator.py
python src\analytics\validate_data.py
```

Validation must report:

- 100,000 records
- 10,000 unique lights
- 1,000 lights per zone
- no missing values
- no duplicate rows
- no light changing zones or coordinates

## 2. HDFS

Start HDFS, then create the project directories and upload the historical CSV:

```powershell
hdfs dfs -mkdir -p /smart-street-lighting/historical
hdfs dfs -mkdir -p /smart-street-lighting/stream
hdfs dfs -put -f data/sample/street_light_data.csv /smart-street-lighting/historical/
```

## 3. Spark

Set the Windows environment for the installed tools before running Spark:

```powershell
$env:JAVA_HOME = "C:\java\jdk1.8.0_202"
$env:HADOOP_HOME = "C:\hadoop-3.3.1"
$env:SPARK_HOME = "$env:USERPROFILE\spark-3.3.4-bin-hadoop3"
$env:PATH = "$env:SPARK_HOME\bin;$env:HADOOP_HOME\bin;$env:PATH"
$env:PYSPARK_PYTHON = "$env:USERPROFILE\smart-street-lighting-energy-optimization\.venv\Scripts\python.exe"
$env:PYSPARK_DRIVER_PYTHON = "$env:USERPROFILE\smart-street-lighting-energy-optimization\.venv\Scripts\python.exe"
```

Example:

```powershell
spark-submit src\spark\process_data.py
```

The analytics scripts read from `/smart-street-lighting/historical/street_light_data.csv`.

## 4. Kafka

Create the topic once:

```powershell
kafka-topics.bat --create --topic street-light-data --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
```

Send the sample dataset:

```powershell
python src\kafka\producer.py
```

Consume/test:

```powershell
python src\kafka\consumer.py
```

For Kafka → HDFS ingestion (restart-safe):

```powershell
python src\hdfs\kafka_to_hdfs.py
```

The stream output is a directory of deterministic JSON batch files under `/smart-street-lighting/stream`. Verify it with:

```powershell
python src\hdfs\verify_stream.py
```

The verifier reports the number of JSON records and fails if duplicate records are detected.

Use a fresh consumer group for a full historical transfer. The sink uses manual Kafka commits and deterministic per-partition HDFS batch files. Restarting the sink is idempotent: a batch already written to HDFS is detected by its filename and is not written twice.

## 5. Hive

The Hive table should point to:

`hdfs://localhost:9000/smart-street-lighting/historical`

Use the schema in the project documentation/setup notes when creating the external table.

## 6. Dashboard API

Start the local dashboard API from the project root:

```powershell
python src\dashboard\smart_lighting_dashboard_server.py
```

It listens on `http://127.0.0.1:8000`. Example endpoint:

`http://127.0.0.1:8000/api/kpis`

Filters are query parameters:

`?location=Zone_07&traffic=All&light_status=All&fault_status=All`

## 7. Grafana

Install the Grafana Infinity datasource and import:

`dashboard/smart_street_lighting_grafana.json`

When importing, select the Infinity datasource. Keep the dashboard API server running while Grafana queries it.

## Project folders

```text
config/
 dashboard/
 data/sample/
 docs/
 notebooks/
 src/
   analytics/
   common/
   dashboard/
   fault_detection/
   hdfs/
   kafka/
   optimization/
   simulator/
   spark/
 tests/
```

Generated runtime data under `data/processed` and `data/dashboard` is intentionally excluded from Git.
