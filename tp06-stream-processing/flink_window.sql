-- Flink SQL Client. Install the matching Kafka SQL connector first.
SET 'table.local-time-zone' = 'UTC';
SET 'table.exec.source.idle-timeout' = '5 s';
SET 'sql-client.execution.result-mode' = 'tableau';

CREATE TABLE campus_events (
    eventId INT,
    studentId STRING,
    eventType STRING,
    department STRING,
    eventTime TIMESTAMP_LTZ(3),
    WATERMARK FOR eventTime AS eventTime - INTERVAL '30' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'campus-events',
    'properties.bootstrap.servers' = 'localhost:9092',
    'properties.group.id' = 'tp06-flink',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json',
    'json.timestamp-format.standard' = 'ISO-8601'
);

SELECT window_start, window_end, department, COUNT(*) AS checkins
FROM TABLE(
    TUMBLE(TABLE campus_events, DESCRIPTOR(eventTime), INTERVAL '1' MINUTE)
)
WHERE eventType = 'check-in'
GROUP BY window_start, window_end, department;
