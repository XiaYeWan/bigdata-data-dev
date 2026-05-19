#!/bin/bash
# ============================================================
# 创建 Kafka Topic — 实时数据通道
# 执行: bash create_topics.sh
# ============================================================
set -e

KAFKA_HOME=/opt/module/kafka_2.12-3.6.1
BROKERS="master:9092,slave1:9092,slave2:9092"

echo "=== 创建 Kafka Topic ==="

# 1. 用户行为日志 Topic (3分区, 2副本)
echo "[1/2] user-behavior-log"
$KAFKA_HOME/bin/kafka-topics.sh --create \
    --bootstrap-server $BROKERS \
    --topic user-behavior-log \
    --partitions 3 \
    --replication-factor 2 \
    2>/dev/null || echo "    Topic 已存在, 跳过"

# 2. 订单实时流 Topic
echo "[2/2] order-stream"
$KAFKA_HOME/bin/kafka-topics.sh --create \
    --bootstrap-server $BROKERS \
    --topic order-stream \
    --partitions 3 \
    --replication-factor 2 \
    2>/dev/null || echo "    Topic 已存在, 跳过"

echo ""
echo "=== Topic 列表 ==="
$KAFKA_HOME/bin/kafka-topics.sh --list --bootstrap-server $BROKERS

echo ""
echo "=== Topic 详情 ==="
$KAFKA_HOME/bin/kafka-topics.sh --describe --bootstrap-server $BROKERS --topic user-behavior-log
$KAFKA_HOME/bin/kafka-topics.sh --describe --bootstrap-server $BROKERS --topic order-stream

echo ""
echo "✅ Kafka Topic 创建完成"
