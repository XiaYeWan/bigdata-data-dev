#!/bin/bash
# ============================================================
# 数据管道一键启动
# 执行: bash start_data_pipeline.sh
# ============================================================
set -e
MOD=/opt/module
GREEN='\033[32m'
NC='\033[0m'

echo "============================================"
echo "  电商数据管道启动"
echo "============================================"

# 1. MySQL 建表 + 造数据
echo -e "\n${GREEN}[Step 1]${NC} MySQL 建表 + 造测试数据"
mysql -uroot -pRoot@123456 < /root/bigdata-data-dev/01-business-data/mysql_business_tables.sql
mysql -uroot -pRoot@123456 < /root/bigdata-data-dev/01-business-data/generate_test_data.sql
echo "  数据量:"
mysql -uroot -pRoot@123456 ecommerce -e "
    SELECT 'dim_user' AS tbl, COUNT(*) AS cnt FROM dim_user UNION ALL
    SELECT 'dim_product', COUNT(*) FROM dim_product UNION ALL
    SELECT 'fact_order', COUNT(*) FROM fact_order UNION ALL
    SELECT 'fact_order_detail', COUNT(*) FROM fact_order_detail UNION ALL
    SELECT 'ods_user_behavior', COUNT(*) FROM ods_user_behavior;
"

# 2. DataX 全量同步
echo -e "\n${GREEN}[Step 2]${NC} DataX MySQL → HDFS"

# 创建 HDFS 目录
HDFS="$MOD/hadoop-3.3.6/bin/hdfs"
for dir in user product orders; do
    $HDFS dfs -mkdir -p "/data_warehouse/ods/$dir/full" 2>/dev/null || true
done

PROJ=/root/bigdata-data-dev/02-datax-sync
DATAX_HOME=${MOD}/datax

echo "  同步用户表..."
python3 $DATAX_HOME/bin/datax.py $PROJ/mysql_to_hdfs_user.json 2>&1 | tail -3

echo "  同步商品表..."
python3 $DATAX_HOME/bin/datax.py $PROJ/mysql_to_hdfs_product.json 2>&1 | tail -3

echo "  同步订单表..."
python3 $DATAX_HOME/bin/datax.py $PROJ/mysql_to_hdfs_orders.json 2>&1 | tail -3

# 验证 HDFS
echo "  HDFS 文件:"
$HDFS dfs -ls -R /data_warehouse/ods/ 2>/dev/null | grep -v "^d" | head -20

# 3. Kafka Topic
echo -e "\n${GREEN}[Step 3]${NC} Kafka Topic 创建"
bash /root/bigdata-data-dev/03-kafka-setup/create_topics.sh

# 4. 启动实时生产者 (后台)
echo -e "\n${GREEN}[Step 4]${NC} 启动 Kafka 实时数据生产者"
pkill -f kafka_producer.py 2>/dev/null || true
sleep 1

nohup python3 /root/bigdata-data-dev/03-kafka-setup/kafka_producer.py \
    --topic user-behavior-log --rate 3 \
    > /tmp/kafka_behavior.log 2>&1 &

nohup python3 /root/bigdata-data-dev/03-kafka-setup/kafka_producer.py \
    --topic order-stream --rate 2 \
    > /tmp/kafka_order.log 2>&1 &

sleep 3
echo "  检测生产者:"
ps aux | grep kafka_producer | grep -v grep

echo ""
# 5. Hive ODS 建外表
echo -e "\n${GREEN}[Step 5]${NC} Hive ODS 外表映射"
/opt/module/hive-3.1.3/bin/hive --hiveconf hive.exec.mode.local.auto=true \
    -f /root/bigdata-data-dev/05-scripts/hive_ods_ddl.sql 2>&1 | grep -E "row_cnt|order_cnt|OK$|^{" | head -10

echo ""
echo "============================================"
echo -e "  ${GREEN}数据管道启动完成!${NC}"
echo "============================================"
echo ""
echo "  管道状态:"
echo "    MySQL:      master:3306 (ecommerce 库)"
echo "    DataX:      HDFS /data_warehouse/ods/ (3表)"
echo "    Hive ODS:   外表已映射 (ods.dim_user/dim_product/fact_order)"
echo "    Kafka:      实时数据生产中 (user-behavior-log / order-stream)"
echo ""
echo "  下一步:"
echo "    数仓分层建模 → ODS → DWD → DWS → ADS"
echo "    Flink 流处理 → 04-flink-stream/flink_sql_job.sql"
