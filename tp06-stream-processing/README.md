# TP06 · Real-Time Campus Attendance

University of Eloued · Master 2 IA & Data Science · Big Data Analytics · 2026/2027

Synthetic QR/RFID check-ins travel through the TP05 Kafka broker to Spark Structured Streaming. Spark counts check-ins by department and classifies each student's first valid daily check-in. The Flink SQL extension calculates final one-minute department counts.

## Versions

| Component | Version |
| --- | --- |
| Kafka broker in TP05 | `apache/kafka:3.9.1`, KRaft |
| Windows Python and producer client | Python 3.11.9, `kafka-python==2.0.2` |
| Spark and Scala | Spark/PySpark 3.5.3, Scala 2.12.18 |
| Spark Kafka package | `org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3` |
| Spark Docker runtime | `apache/spark:3.5.3-python3`, Temurin Java 11, bundled Python 3.8.10 |
| Flink SQL runtime and connector | Flink 1.20.2, Scala 2.12, Java 11, Kafka SQL connector `3.4.0-1.20` |

## Start the lab on Windows

Start Docker Desktop. From the TP05 Compose directory, run `docker compose up -d`. Then open PowerShell inside `tp06-stream-processing`.

```powershell
docker exec tp05-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic campus-events --partitions 3 --replication-factor 1
docker exec tp05-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic campus-events
py -3.11 -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
```

Run Spark in one terminal. Wait for `Ready Spark` before running the producer in a second terminal.

```powershell
pwsh -NoProfile -File .\run-spark.ps1
```

```powershell
.\.venv\Scripts\python.exe producer.py
```

The producer sends seven records. Six are check-ins and one is an `open-course` event. Its final check-in has an earlier event timestamp than two previously sent records. The Spark query prints updated aggregate rows, and the progress line reports the batch, input count, processing rate, watermark, and rows dropped by the watermark.

The completed run already populated `campus-events`. To see exactly seven fresh events locally, create a new three-partition demo topic and pass its name with `--topic` to both commands. Give Spark a fresh path such as `--checkpoint checkpoint/demo`.

The Docker runner joins the TP05 broker's network namespace, so the existing advertised `localhost:9092` listener works from Spark. The Spark container mounts this folder for source and checkpoints. The Windows producer runs in the virtual environment. On a Linux host with Kafka reachable at the configured address and PySpark 3.5.3 installed, the equivalent Spark launch is:

```bash
spark-submit --master 'local[2]' \
  --conf spark.jars.ivySettings="$PWD/ivysettings.xml" \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 spark_stream.py
```

## Experiments

Use `--topic` on both scripts to isolate experiments with fresh three-partition topics. Create each topic using the Kafka command above with the new name. A new checkpoint and topic give a clean comparison on later runs. Keep the original checkpoint and topic for recovery.

| Experiment | Arguments after `run-spark.ps1` |
| --- | --- |
| Default query | No extra arguments |
| Five-second trigger | `--checkpoint checkpoint/trigger-5s --trigger "5 seconds"` |
| Two-minute tumbling windows | `--checkpoint checkpoint/tumbling-2m --window "2 minutes"` |
| Two-minute sliding windows, one-minute slide | `--checkpoint checkpoint/sliding-1m --window "2 minutes" --slide "1 minute"` |
| Two-minute sliding windows, 30-second slide | `--checkpoint checkpoint/sliding-30s --window "2 minutes" --slide "30 seconds"` |
| Two-minute watermark | `--checkpoint checkpoint/watermark-2m --watermark "2 minutes" --trigger "5 seconds"` |
| Campus attendance | `--checkpoint checkpoint/attendance --mode attendance` |
| Counts by event type and department | `--checkpoint checkpoint/event-types --mode event-types` |

For a fixed input comparison, publish the sample only once, then pass `--available-now` to the window variants to process the same retained input and exit. For the trigger comparison, publish with `--interval 2` while each query is running.

For recovery, stop the default query with Ctrl+C after the sample has finished processing. Restart the identical command with `checkpoint/campus-events`, then publish `producer.py --profile resume`. A single new Physics check-in updates the previous count of two to three when the topic started empty. For repeat runs, use the same `--date YYYY-MM-DD` on all producer commands and a fresh three-partition topic/checkpoint pair for each complete recovery test.

For the watermark comparison, use separate topics and checkpoints with `--watermark "10 seconds"`, `"30 seconds"`, `"60 seconds"`, or `"2 minutes"`. With the five-second trigger running, publish the sample with `--interval 0` and wait for it to process. Send `--profile advance --interval 0` and wait until the next reported watermark reflects the 07:16:40 UTC event timestamp. Then send `--profile delayed --interval 0`. The late event's one-minute window ends at 07:16:00 UTC.

For the 100+ event experiment, run the producer with `--profile mixed --count 104 --interval 0` and compare its publish rate with the Spark progress line for `--mode event-types`. Use a new topic and checkpoint for those commands.

## First check-in after 08:15

The producer emits UTC timestamps with a `Z` suffix. Attendance converts each timestamp to `Africa/Algiers` and retains the earliest valid check-in for each student and local calendar day. A timestamp strictly after 08:15:00 is `LATE`. Exactly 08:15:00 is `ON TIME`. Updated rows include the student ID, department, source event timestamp, local time, and status. If an earlier check-in arrives later, the next update can correct the status. This compact attendance query keeps one state row per student per day for the lab.

## Flink SQL

The runnable query is in [flink_window.sql](flink_window.sql). The Docker extension uses the same Kafka listener as Spark. Start it in a separate terminal after the sample is in Kafka.

```powershell
pwsh -NoProfile -File .\run-flink.ps1
```

In the producer terminal, run `.\.venv\Scripts\python.exe producer.py --profile advance --interval 0`. Use the same synthetic date as the sample. The new records advance event time so Flink can close the two earlier windows. Stop with Ctrl+C and, if the named Flink container is still present, run `docker rm -f tp06-flink`.

## Report and evidence

See [REPORT.md](REPORT.md) for the observed results, report questions, and screenshot index. The screenshots display saved console output from the Docker and Python runs.

Stop Spark with Ctrl+C. Keep checkpoints for the recovery demonstration. When finished with Kafka, run `docker compose down` from the TP05 project to stop it and retain its stored data. The virtual environment, local connector caches, and Spark checkpoints are excluded from Git.
