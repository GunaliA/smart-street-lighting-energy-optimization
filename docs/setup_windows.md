# Windows Setup Checklist

1. Install Java 8 JDK and set `JAVA_HOME`.
2. Configure Hadoop 3.3.1 and Windows native `winutils.exe`/`hadoop.dll`.
3. Start HDFS and verify `hdfs dfs -ls /`.
4. Install Python 3.11 and create `.venv`.
5. Install `requirements.txt`.
6. Install Spark 3.3.4 Hadoop 3 and set `SPARK_HOME`.
7. Install Kafka 3.9.2 and start the KRaft broker.
8. Install Hive 3.1.3 and initialize its Derby metastore if required.
9. Install Grafana and the Infinity datasource.
10. Run the project in the order described in `README.md`.

The project is software-only; no ESP32, LDR, PIR, temperature sensor, or other physical hardware is required.


## Kafka → HDFS restart-safe ingestion

The Kafka sink uses consumer group `hdfs-sink-v3`. It writes deterministic JSON batch files named by Kafka partition and offset range. If the process is restarted after an HDFS write but before a Kafka offset commit, the existing batch file is detected and the batch is not duplicated.

Run:

```powershell
python src\hdfs\kafka_to_hdfs.py
```

Validate the stream:

```powershell
python src\hdfs\verify_stream.py
```

The expected record count equals the Kafka topic's record count for the transfer being validated. Do not run the producer repeatedly against the same topic unless duplicate historical messages are intentionally wanted.
