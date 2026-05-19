-- ============================================================
-- Flink SQL 实时流处理
-- 用法: 启动 Flink SQL Client 后逐段执行
-- $FLINK_HOME/bin/sql-client.sh embedded
-- ============================================================

-- 1. 创建 Kafka Source 表 (消费用户行为日志)
CREATE TABLE behavior_source (
    log_id         BIGINT,
    user_id        BIGINT,
    product_id     BIGINT,
    behavior_type  STRING,
    behavior_time  TIMESTAMP(3),
    session_id     STRING,
    ip             STRING,
    -- Kafka 消息时间戳作为事件时间
    event_time AS behavior_time,
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'user-behavior-log',
    'properties.bootstrap.servers' = 'master:9092,slave1:9092,slave2:9092',
    'properties.group.id' = 'flink-behavior-consumer',
    'scan.startup.mode' = 'latest-offset',
    'format' = 'json'
);

-- 2. 创建 Kafka Source 表 (消费订单流)
CREATE TABLE order_source (
    order_id        BIGINT,
    user_id         BIGINT,
    order_status    STRING,
    order_amount    DOUBLE,
    discount_amount DOUBLE,
    actual_amount   DOUBLE,
    payment_method  STRING,
    order_time      TIMESTAMP(3),
    items           ARRAY<ROW<product_id BIGINT, quantity INT, unit_price DOUBLE, subtotal DOUBLE>>,
    event_time AS order_time,
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'order-stream',
    'properties.bootstrap.servers' = 'master:9092,slave1:9092,slave2:9092',
    'properties.group.id' = 'flink-order-consumer',
    'scan.startup.mode' = 'latest-offset',
    'format' = 'json'
);

-- 3. 实时汇总: 每分钟各品类销量 (Sink 到控制台)
SELECT
    TUMBLE_START(event_time, INTERVAL '1' MINUTE) AS window_start,
    behavior_type,
    COUNT(DISTINCT user_id)   AS uv,
    COUNT(*)                  AS pv
FROM behavior_source
GROUP BY TUMBLE(event_time, INTERVAL '1' MINUTE), behavior_type;

-- 4. 实时汇总: 每分钟订单金额 (Sink 到 HDFS)
-- 需要先创建 HDFS Sink 表，此处展示逻辑
CREATE TABLE order_agg_sink (
    window_start   TIMESTAMP(3),
    window_end     TIMESTAMP(3),
    order_count    BIGINT,
    total_amount   DOUBLE,
    avg_amount     DOUBLE
) WITH (
    'connector' = 'filesystem',
    'path' = 'hdfs://master:9000/data_warehouse/realtime/order_agg',
    'format' = 'json'
);

INSERT INTO order_agg_sink
SELECT
    TUMBLE_START(event_time, INTERVAL '1' MINUTE) AS window_start,
    TUMBLE_END(event_time, INTERVAL '1' MINUTE)   AS window_end,
    COUNT(*)                                      AS order_count,
    SUM(actual_amount)                            AS total_amount,
    AVG(actual_amount)                            AS avg_amount
FROM order_source
GROUP BY TUMBLE(event_time, INTERVAL '1' MINUTE);
