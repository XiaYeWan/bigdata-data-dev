#!/bin/bash
# ============================================================
# Spark 批处理一键运行脚本
# 用法: bash spark_batch_runner.sh [all|etl|user|product|ml]
# ============================================================

set -e

SPARK_HOME="/opt/module/spark-3.4.3-bin-hadoop3"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SPARK_SUBMIT="${SPARK_HOME}/bin/spark-submit"

# 3节点7.6G物理机保守配置
SPARK_OPTS="--master yarn --deploy-mode client --driver-memory 512m --executor-memory 1g --executor-cores 1 --num-executors 2"
declare -A JOBS=(
    ["etl"]="spark_etl_warehouse.py|Spark ETL 数仓四层管道"
    ["user"]="spark_user_analysis.py|用户画像分析 & RFM分层"
    ["product"]="spark_product_analysis.py|商品销售分析 & 关联规则"
    ["ml"]="spark_ml_recommend.py|MLlib ALS推荐 + KMeans聚类"
)

# 颜色
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${GREEN}"
echo "╔══════════════════════════════════════════════════════╗"
echo "║     ⚡ Spark Batch Processing Runner               ║"
echo "║     Spark 3.4.3 on YARN | 3-Node Cluster           ║"
echo "╚══════════════════════════════════════════════════════╝"
echo -e "${NC}"

# 前置检查
check_prerequisites() {
    echo "📋 Checking prerequisites..."

    # Hadoop
    if ! hdfs dfs -ls / 2>/dev/null | grep -q .; then
        echo -e "${RED}❌ HDFS not accessible${NC}"
        exit 1
    fi
    echo "   ✅ HDFS OK"

    # YARN
    RUNNING_APPS=$(yarn application -list -appStates RUNNING 2>/dev/null | grep -c RUNNING || echo "0")
    echo "   ✅ YARN OK (${RUNNING_APPS} running apps)"

    # Spark
    if [ ! -f "${SPARK_SUBMIT}" ]; then
        echo -e "${RED}❌ Spark not found at ${SPARK_HOME}${NC}"
        exit 1
    fi
    echo "   ✅ Spark OK (${SPARK_HOME})"

    # Hive Metastore
    if ! ps aux | grep -v grep | grep -q metastore; then
        echo -e "${YELLOW}⚠️  Hive Metastore not running, starting...${NC}"
        nohup /opt/module/hive-3.1.3/bin/hive --service metastore > /tmp/hive-metastore.log 2>&1 &
        sleep 5
    fi
    echo "   ✅ Hive Metastore OK"

    # ODS 数据检查
    USER_CNT=$(/opt/module/hive-3.1.3/bin/hive -e "SELECT COUNT(*) FROM ods.dim_user" 2>/dev/null | tail -1 | tr -d ' ')
    if [ -z "$USER_CNT" ] || [ "$USER_CNT" -lt 100 ]; then
        echo -e "${RED}❌ ODS data missing (dim_user count: ${USER_CNT:-0})${NC}"
        echo "   Run: /opt/module/hive-3.1.3/bin/hive -f 05-scripts/hive_ods_ddl.sql"
        exit 1
    fi
    echo "   ✅ ODS data OK (dim_user: ${USER_CNT})"

    echo ""
}

run_job() {
    local job_key=$1
    local job_info="${JOBS[$job_key]}"
    local py_file="${job_info%%|*}"
    local desc="${job_info##*|}"

    echo -e "${GREEN}"
    echo "┌──────────────────────────────────────────────────────┐"
    echo "│  🚀 Running: ${desc}"
    echo "└──────────────────────────────────────────────────────┘"
    echo -e "${NC}"

    START_TIME=$(date +%s)

    ${SPARK_SUBMIT} ${SPARK_OPTS} "${SCRIPT_DIR}/${py_file}"

    END_TIME=$(date +%s)
    DURATION=$((END_TIME - START_TIME))

    echo -e "${GREEN}✅ ${desc} — Done in ${DURATION}s${NC}"
    echo ""
}

# ============================================================
# Main
# ============================================================
check_prerequisites

MODE="${1:-all}"

case "${MODE}" in
    all)
        echo "📦 Running ALL Spark batch jobs..."
        echo ""
        run_job "etl"
        run_job "user"
        run_job "product"
        run_job "ml"
        ;;
    etl)
        run_job "etl"
        ;;
    user)
        run_job "user"
        ;;
    product)
        run_job "product"
        ;;
    ml)
        run_job "ml"
        ;;
    *)
        echo "Usage: bash spark_batch_runner.sh [all|etl|user|product|ml]"
        echo ""
        echo "  all      - Run all 4 jobs (default)"
        echo "  etl      - ETL warehouse pipeline (ODS→DWD→DWS→ADS)"
        echo "  user     - User profiling & RFM analysis"
        echo "  product  - Product sales & association"
        echo "  ml       - ALS recommendation & K-Means clustering"
        exit 1
        ;;
esac

# 输出结果汇总
echo -e "${GREEN}"
echo "╔══════════════════════════════════════════════════════╗"
echo "║     ✅ All Spark Batch Jobs Complete               ║"
echo "║     Results in /data_warehouse/ads/                ║"
echo "╚══════════════════════════════════════════════════════╝"
echo -e "${NC}"

echo "📁 Output directories:"
hdfs dfs -ls /data_warehouse/ads/ 2>/dev/null || echo "   (check HDFS)"
echo ""
echo "📊 Next Steps:"
echo "   1. Verify results: hdfs dfs -cat /data_warehouse/ads/spark_batch/daily_trade/part-* | head"
echo "   2. Import to Superset for visualization"
echo "   3. Schedule with DolphinScheduler (cron: 0 3 * * *)"
