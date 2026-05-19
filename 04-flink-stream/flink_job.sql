SET 'sql-client.execution.result-mode' = 'tableau';

CREATE TABLE behavior_source (
    log_id         BIGINT,
    user_id        BIGINT,
    product_id     BIGINT,
    behavior_type  STRING,
    behavior_time  TIMESTAMP(3),
    session_id     STRING,
    ip             STRING,
    event_time AS behavior_time,
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'user-behavior-log',
    'properties.bootstrap.servers' = 'master:9092',
    'properties.group.id' = 'flink-sql-v11',
    'scan.startup.mode' = 'latest-offset',
    'format' = 'json'
);

SELECT
    TUMBLE_START(event_time, INTERVAL '1' MINUTE) AS window_start,
    behavior_type,
    COUNT(DISTINCT user_id) AS uv,
    COUNT(*) AS pv
FROM behavior_source
GROUP BY TUMBLE(event_time, INTERVAL '1' MINUTE), behavior_type;
