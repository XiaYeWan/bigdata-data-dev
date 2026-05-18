#!/bin/bash
# ============================================================
# 数据验证脚本
# 执行: bash check_data.sh
# ============================================================
echo "============================================"
echo "  数据管道验证"
echo "============================================"

# MySQL 数据量
echo -e "\n>>> MySQL 数据量"
mysql -uroot -pRoot@123456 ecommerce -e "
    SELECT 'dim_user' AS table_name, COUNT(*) AS row_count FROM dim_user
    UNION ALL
    SELECT 'dim_product', COUNT(*) FROM dim_product
    UNION ALL
    SELECT 'fact_order', COUNT(*) FROM fact_order
    UNION ALL
    SELECT 'fact_order_detail', COUNT(*) FROM fact_order_detail
    UNION ALL
    SELECT 'ods_user_behavior', COUNT(*) FROM ods_user_behavior
    ORDER BY table_name;
"

# HDFS 文件
echo -e "\n>>> HDFS ODS 层文件"
/opt/module/hadoop-3.3.6/bin/hdfs dfs -ls -R /data_warehouse/ods/ 2>/dev/null | grep -E "^-" || echo "  ⚠️ 未找到文件, 请先运行 DataX 同步"

# Kafka Topic 消息量
echo -e "\n>>> Kafka Topic"
KAFKA_HOME=/opt/module/kafka_2.12-3.6.1
$KAFKA_HOME/bin/kafka-topics.sh --list --bootstrap-server master:9092,slave1:9092,slave2:9092

echo -e "\n  user-behavior-log 最新消息:"
$KAFKA_HOME/bin/kafka-console-consumer.sh --bootstrap-server master:9092,slave1:9092,slave2:9092 \
    --topic user-behavior-log --max-messages 2 --timeout-ms 5000 2>/dev/null || echo "  (暂无数据)"

echo -e "\n  order-stream 最新消息:"
$KAFKA_HOME/bin/kafka-console-consumer.sh --bootstrap-server master:9092,slave1:9092,slave2:9092 \
    --topic order-stream --max-messages 2 --timeout-ms 5000 2>/dev/null || echo "  (暂无数据)"

echo -e "\n============================================"
echo "  验证完成"
echo "============================================"
