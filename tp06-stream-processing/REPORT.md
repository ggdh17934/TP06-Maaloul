# TP06 short report · Real-time campus attendance

University of Eloued · Master 2 IA & Data Science · Big Data Analytics · 2026/2027

## Setup and baseline

The lab reused the TP05 Kafka 3.9.1 KRaft broker. The new `campus-events` topic had three partitions and replication factor one. The producer used Python 3.11.9 and kafka-python 2.0.2. Spark 3.5.3 ran locally with two worker threads in Docker, using Scala 2.12.18 and the Spark 3.5.3 Kafka connector for Scala 2.12. The counts query used UTC, one-minute tumbling windows, a 30-second watermark and update output mode. Optional Flink execution used Flink 1.20.2 and Kafka SQL connector 3.4.0-1.20.

The initial run published seven events. Spark counted six check-ins and excluded the `open-course` event. With UTC window boundaries, it produced these cumulative values:

| UTC window, 14 Nov 2026 | Computer Science | Mathematics | Physics |
| --- | ---: | ---: | ---: |
| 07:14–07:15 | 1 | 1 | 0 |
| 07:15–07:16 | 1 | 1 | 2 |

Spark prints a row when an aggregate changes, so the zero in the table represents a department with no row in that window. Event 7 was published after event 6 but carried an earlier event timestamp. Its Physics check-in still contributed to the 07:15–07:16 group.

## Controlled experiments

| Experiment | Observed result and interpretation |
| --- | --- |
| A · Trigger | The default and five-second runs each handled a one-record first batch followed by a six-record batch. The first two batch starts were about 13.25 seconds apart with the default and 13.08 seconds apart with the five-second setting, measured from the checkpoint batch timestamps. Startup work exceeded the configured interval. The default polls again when free, and a configured interval schedules opportunities to start a batch. A batch already running can push the next batch past its target time. |
| B · Tumbling windows | With the same seven source events, one-minute windows produced five department/window rows. A two-minute window merged them into 07:14–07:16 UTC with three rows, each with two check-ins. |
| C · Sliding windows | Two-minute windows advancing each minute produced 8 department/window rows and assigned each of the six check-ins to two overlapping windows. Advancing every 30 seconds produced 15 rows and assigned each check-in to four windows. Totals across overlapping windows therefore count the same check-ins multiple times. |
| D · Watermark | The delayed event belonged to a window ending at 07:16:00 UTC. See the comparison below for the watermark and result in each run. |
| E · Recovery | After stopping the first Spark process, the same checkpoint resumed at batch 2. The new Physics check-in advanced partition 0's next offset from 3 to 4; the other offsets stayed at 2. The recovered count rose from 2 to 3 for 07:15–07:16. Spark reused the prior aggregate and consumed only the new record. |

Each lateness run used its own topic and checkpoint. The producer sent the sample, then a 07:16:40 event to each partition, then the delayed Physics event with timestamp 07:15:02. The delayed event was handled in a distinct micro-batch.

| Watermark delay | Watermark when delayed event was processed (UTC) | Delayed event |
| --- | --- | --- |
| 10 seconds | 07:16:30 | Dropped |
| 30 seconds | 07:16:10 | Dropped |
| 60 seconds | 07:15:40 | Physics count updated |
| 2 minutes | 07:14:40 | Physics count updated |

A longer delay gives old windows more time to receive records and keeps their state longer. In update mode, the console can print interim counts while windows are open. In append mode, a window row is emitted after the watermark closes it. With no advancing event timestamps, waiting in wall-clock time alone cannot close more event-time windows.

### Attendance test

The threshold was strictly later than 08:15:00 in `Africa/Algiers`. The query evaluated five distinct students in the sample. S101 and S204 were on time. S204's second check-in occurred at 08:15:38 local, while the earlier 08:14:58 check-in determined the daily status. S305, S411, and S512 were late at 08:15:12, 08:15:25, and 08:15:02 respectively. Using event time lets the classification reflect when the scanner recorded the check-in, even if Kafka delivery or Spark processing occurs later. The update result can correct a late status when an earlier valid check-in arrives.

### Further investigation

The mixed run sent 104 records in approximately 0.42 seconds, averaging 250.41 Kafka acknowledgements per second. Spark processed all 104 in its first micro-batch and reported 5.80 input rows processed per second, with zero rows dropped. The Spark rate includes cold-start micro-batch work and the stateful aggregation. These measurements describe a short producer burst and a backlog read, so they cannot establish sustained throughput. Grouping by minute, department and event type produced `library-entry` and `open-course` rows alongside check-ins. The `mixed` pattern sent 78 check-ins, 13 course events, and 13 library entries.

The independent window queries read the same retained events concurrently with separate checkpoint directories. Each directory belongs to one query because it stores that query's offsets, batch metadata, and state schema. Sharing it would mix progress and state for separate jobs.

A five-minute session could group all events for each student by extending a session whenever the next event arrives within five minutes of the preceding event, using event time. A longer gap starts another session. Each student's activity determines the session boundaries and length. Tumbling and sliding boundaries are fixed by clock alignment and configuration.

## Flink SQL analysis and observed output

1. `eventTime` is the event-time attribute. The table uses `TIMESTAMP_LTZ(3)` with the JSON ISO-8601 format to parse the producer's UTC `Z` timestamps and displays them in UTC.
2. The watermark expression puts source event-time progress 30 seconds behind the timestamps it observes. Downstream progress depends on active inputs, so the SQL also allows idle partitions to be marked idle after five seconds.
3. Each tumbling window lasts one minute. One valid event belongs to one such interval.
4. `window_start` and `window_end` identify the interval represented by each department count. They support chronological display and make counts from different intervals distinguishable.

The Flink SQL Client emitted two final rows for 07:14–07:15 and three for 07:15–07:16. Physics was 3 in the second window because Flink consumed the recovery event as well. Publishing the three 07:16:40 UTC check-ins moved event time far enough to close both earlier windows. The latest window stayed open. See the [Flink output](screenshots/flink_output.png).

## Report questions

1. **Roles.** Kafka transports and durably retains partitioned events for independent consumers. Spark and Flink parse those events, apply filters and business logic, and maintain aggregations over the stream.
2. **Incremental results.** Spark models the input as an unbounded table. Each micro-batch contributes new rows, and Spark updates the affected portions of the state and result table.
3. **Micro-batches.** A micro-batch is a finite set of newly available input records processed together as a Spark job. Batching amortizes scheduling and data-processing overhead. The waiting and scheduling time can add latency for individual records.
4. **Kafka source settings.** `subscribe` selects the topic. `startingOffsets=earliest` reads from the earliest retained offsets when a query has a fresh checkpoint. A restarted query resumes using the checkpoint offsets.
5. **JSON schema.** Declaring the fields and types gives Spark a stable streaming plan and lets it parse Kafka values into usable columns. The query rejects missing or invalid event timestamps and requires a nonempty student, department, and event type.
6. **Event time and processing time.** Event 1 records 07:14:52 UTC, equivalent to 08:14:52 campus time. Spark reported its first micro-batch at approximately 17:51:46 UTC on the test machine. The synthetic timestamp chose its window, and the Spark timestamp describes when the engine began processing it.
7. **Watermarks.** A watermark tracks how far event time has progressed and gives the engine a boundary for expiring state. Late records can still arrive after their window is closed and can be dropped. In this aggregation the window end, the current watermark, and the order of micro-batches determine the outcome.
8. **Window types.** Tumbling windows are fixed and disjoint. Sliding windows have a fixed width and step and overlap when the step is smaller than the width. A session groups a key's events by activity and ends after an inactivity gap.
9. **Checkpoints.** Spark persists source offset logs, batch commits, state data, and query metadata. Restarting the compatible query with the same location lets it resume work with the same source progress and aggregate state. A fresh location starts a separate query.
10. **Spark and Flink.** Spark's default execution runs micro-batches, while Flink's streaming runtime processes records continuously. Spark uses the Structured Streaming DataFrame/SQL model in its broader analytics ecosystem; Flink offers DataStream and Table/SQL APIs in a runtime built around event-time state and streaming timers.
11. **Engine choice.** I would use Spark for this attendance lab. A few seconds of latency can serve its monitoring needs, Spark provides event-time windows and checkpointed state, and the team can reuse the Python and Spark analytics stack for later reporting. If the campus later requires consistently lower intervention latency or more complex keyed timers, I would evaluate Flink against the actual workload and team skills.
12. **Evidence.** See the screenshot index below. The images show saved command output from actual runs.

## Problems encountered and solutions

- Docker Desktop was stopped initially. Starting it and bringing up the TP05 Compose project restored the broker with its persistent data volume.
- The Spark image runs as a non-root user. Ivy could not write its original Docker volume mount. The runner now uses a writable local cache and Maven Central to resolve the matching connector.
- The pinned kafka-python client does not support using `KafkaProducer` as a context manager. The producer closes it explicitly and waits for Kafka to acknowledge each message before printing its offset.
- Flink's initial reduced TaskManager memory could not fit its required managed and network memory. The local cluster worked with 1,152 MiB for the TaskManager and 800 MiB for the JobManager.

## Screenshot index

- [Kafka topic](screenshots/kafka_topic.png)
- [Seven producer events](screenshots/producer_output.png)
- [Spark window updates](screenshots/spark_output.png)
- [Checkpoint recovery](screenshots/checkpoint_or_recovery.png)
- [Trigger comparison](screenshots/trigger_comparison.png)
- [Tumbling and sliding windows](screenshots/window_comparison.png)
- [Attendance classification](screenshots/attendance.png)
- [Watermark comparison](screenshots/watermark_comparison.png)
- [104-event run and event types](screenshots/event_types.png)
- [Flink SQL output](screenshots/flink_output.png)

## References

- [Apache Spark Structured Streaming programming guide](https://spark.apache.org/docs/3.5.6/structured-streaming-programming-guide.html)
- [Apache Spark Kafka integration guide](https://spark.apache.org/docs/3.5.6/structured-streaming-kafka-integration.html)
- [Apache Flink 1.20 time attributes](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/table/concepts/time_attributes/)
- [Apache Flink 1.20 window table-valued functions](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/table/sql/queries/window-tvf/)
