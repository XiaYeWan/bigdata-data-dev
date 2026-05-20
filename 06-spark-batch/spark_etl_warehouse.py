#!/usr/bin/env python3
"""
Spark 批处理：数仓 ETL 四层管道
ODS(读HDFS) → DWD(清洗) → DWS(日汇总) → ADS(业务指标)
============================================================
提交方式:
  /opt/module/spark-3.4.3-bin-hadoop3/bin/spark-submit \
    --master yarn \
    --deploy-mode client \
    --driver-memory 512m \
    --executor-memory 1g \
    --executor-cores 1 \
    --num-executors 2 \
    spark_etl_warehouse.py
============================================================
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, sum, avg, max, min, round as spark_round,
    to_date, datediff, current_date, when, row_number, lit
)
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType

import time
import sys

# ============================================================
# Spark Session (enable Hive support to read Hive tables)
# ============================================================
spark = SparkSession.builder \
    .appName("Spark-ETL-Warehouse") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("=" * 60)
print("🚀 Spark ETL Warehouse Pipeline Started")
print("=" * 60)

# ============================================================
# Layer 0: ODS — Read raw data from Hive external tables
# ============================================================
print("\n📥 [ODS] Reading source data...")

# 确保 ODS 库可用
spark.sql("CREATE DATABASE IF NOT EXISTS ods LOCATION '/data_warehouse/ods/'")
spark.sql("USE ods")

t0 = time.time()

# 从 Hive ODS 外表读取（已在 hive_ods_ddl.sql 中定义）
df_user = spark.sql("SELECT * FROM ods.dim_user").cache()
df_product = spark.sql("SELECT * FROM ods.dim_product").cache()
df_order = spark.sql("SELECT * FROM ods.fact_order").cache()

user_cnt = df_user.count()
product_cnt = df_product.count()
order_cnt = df_order.count()

print(f"   dim_user:    {user_cnt:,} rows")
print(f"   dim_product: {product_cnt:,} rows")
print(f"   fact_order:  {order_cnt:,} rows")
print(f"   ⏱  ODS read: {time.time() - t0:.1f}s")

# ============================================================
# Layer 1: DWD — Clean & Standardize
# ============================================================
print("\n🧹 [DWD] Cleaning and standardizing data...")
t1 = time.time()

# 1.1 清洗订单表：过滤异常状态，标准化字段
dwd_order = df_order \
    .filter(col("order_status").isin("paid", "shipped", "completed")) \
    .filter(col("actual_amount") > 0) \
    .withColumn("order_date", to_date(col("order_time"))) \
    .withColumn("is_weekend", when(
        col("order_date").cast("string").substr(1, 1).isin("S"), 1
    ).otherwise(0)) \
    .select(
        "order_id", "user_id", "order_status",
        col("order_amount").cast(DoubleType()).alias("order_amount"),
        col("discount_amount").cast(DoubleType()).alias("discount_amount"),
        col("actual_amount").cast(DoubleType()).alias("actual_amount"),
        "payment_method", "order_date"
    )

dwd_order.cache()
dwd_cnt = dwd_order.count()
print(f"   dwd_order (清洗后): {dwd_cnt:,} rows (剔除 {order_cnt - dwd_cnt} 条异常)")
print(f"   ⏱  DWD clean: {time.time() - t1:.1f}s")

# ============================================================
# Layer 2: DWS — Daily Summary Aggregations
# ============================================================
print("\n📊 [DWS] Computing daily summaries...")
t2 = time.time()

# 2.1 每日交易汇总
dws_daily_trade = dwd_order.groupBy("order_date").agg(
    count("order_id").alias("order_cnt"),
    spark_round(sum("order_amount"), 2).alias("total_gmv"),
    spark_round(sum("discount_amount"), 2).alias("total_discount"),
    spark_round(sum("actual_amount"), 2).alias("total_revenue"),
    spark_round(avg("actual_amount"), 2).alias("avg_order_value"),
    count("user_id").alias("buyer_cnt")
).orderBy("order_date")

dws_daily_trade.cache()
print(f"   dws_daily_trade: {dws_daily_trade.count()} 天")

# 2.2 每日支付方式分布
dws_payment_method = dwd_order.groupBy("order_date", "payment_method").agg(
    count("order_id").alias("order_cnt"),
    spark_round(sum("actual_amount"), 2).alias("total_amount")
).orderBy("order_date", "payment_method")

# 2.3 用户购买力分层
user_spending = dwd_order.groupBy("user_id").agg(
    count("order_id").alias("order_cnt"),
    spark_round(sum("actual_amount"), 2).alias("total_spent"),
    spark_round(avg("actual_amount"), 2).alias("avg_per_order"),
    max("order_date").alias("last_order_date")
)

# RFM 分段
today_date = current_date()
dws_user_rfm = user_spending \
    .withColumn("recency_days", datediff(lit(today_date), col("last_order_date"))) \
    .withColumn("r_score", when(col("recency_days") <= 7, 3)
                         .when(col("recency_days") <= 30, 2)
                         .otherwise(1)) \
    .withColumn("f_score", when(col("order_cnt") >= 10, 3)
                         .when(col("order_cnt") >= 3, 2)
                         .otherwise(1)) \
    .withColumn("m_score", when(col("total_spent") >= 50000, 3)
                         .when(col("total_spent") >= 10000, 2)
                         .otherwise(1)) \
    .withColumn("rfm_total", col("r_score") + col("f_score") + col("m_score")) \
    .withColumn("user_tier", when(col("rfm_total") >= 7, "高价值")
                          .when(col("rfm_total") >= 5, "中价值")
                          .otherwise("低价值"))

dws_user_rfm.cache()
print(f"   dws_user_rfm: {dws_user_rfm.count()} users")

print(f"   ⏱  DWS compute: {time.time() - t2:.1f}s")

# ============================================================
# Layer 3: ADS — Business Application Metrics
# ============================================================
print("\n📈 [ADS] Computing business metrics...")
t3 = time.time()

# 3.1 GMV 趋势 Top 5
print("\n   🏆 Top 5 GMV Days:")
dws_daily_trade.orderBy(col("total_gmv").desc()).show(5, truncate=False)

# 3.2 支付方式占比
print("\n   💳 Payment Method Distribution (All Time):")
dwd_order.groupBy("payment_method").agg(
    count("order_id").alias("order_cnt"),
    spark_round(count("order_id") / dwd_cnt * 100, 1).alias("pct")
).orderBy(col("order_cnt").desc()).show(truncate=False)

# 3.3 用户分层统计
print("\n   👤 User Tier Distribution:")
dws_user_rfm.groupBy("user_tier").agg(
    count("user_id").alias("user_cnt"),
    spark_round(avg("total_spent"), 2).alias("avg_spent"),
    spark_round(avg("order_cnt"), 1).alias("avg_orders")
).orderBy(col("user_cnt").desc()).show(truncate=False)

# 3.4 高价值用户 Top 10
print("\n   👑 Top 10 High-Value Users:")
dws_user_rfm.filter(col("user_tier") == "高价值") \
    .orderBy(col("total_spent").desc()) \
    .select("user_id", "order_cnt", "total_spent", "recency_days", "rfm_total") \
    .show(10, truncate=False)

# 3.5 整体指标汇总
print("\n   📋 Summary Dashboard:")
dwd_order.agg(
    count("order_id").alias("total_orders"),
    spark_round(sum("actual_amount"), 2).alias("total_revenue"),
    spark_round(avg("actual_amount"), 2).alias("avg_order_value"),
    count("user_id").alias("total_buyers")
).show(truncate=False)

print(f"\n   ⏱  ADS compute: {time.time() - t3:.1f}s")

# ============================================================
# Save Results to HDFS (for Superset visualization)
# ============================================================
print("\n💾 Saving ADS results to HDFS...")
t4 = time.time()

output_base = "/data_warehouse/ads/spark_batch"

# 保存每日交易 (供 Superset 图表使用)
dws_daily_trade.coalesce(1).write \
    .mode("overwrite") \
    .option("header", "true") \
    .option("delimiter", "\t") \
    .csv(f"{output_base}/daily_trade")

# 保存用户分层
dws_user_rfm.coalesce(1).write \
    .mode("overwrite") \
    .option("header", "true") \
    .option("delimiter", "\t") \
    .csv(f"{output_base}/user_rfm")

# 保存支付方式分布
dws_payment_method.coalesce(1).write \
    .mode("overwrite") \
    .option("header", "true") \
    .option("delimiter", "\t") \
    .csv(f"{output_base}/payment_dist")

print(f"   Saved: {output_base}/daily_trade/")
print(f"   Saved: {output_base}/user_rfm/")
print(f"   Saved: {output_base}/payment_dist/")
print(f"   ⏱  Save time: {time.time() - t4:.1f}s")

# ============================================================
# Cleanup
# ============================================================
df_user.unpersist()
df_product.unpersist()
df_order.unpersist()
dwd_order.unpersist()
dws_daily_trade.unpersist()
dws_user_rfm.unpersist()

total_time = time.time() - t0
print(f"\n{'=' * 60}")
print(f"✅ Spark ETL Pipeline Completed — Total: {total_time:.1f}s")
print(f"{'=' * 60}")

spark.stop()
