#!/usr/bin/env python3
"""
Spark 批处理：用户画像分析 & RFM 精细化分层
============================================================
使用 DataFrame API 对用户维度进行深度分析：
  - 用户地理分布 (City-level)
  - VIP 等级消费力分析
  - RFM 精细化分群 (9宫格)
  - 复购率 & 用户生命周期

提交方式:
  /opt/module/spark-3.4.3-bin-hadoop3/bin/spark-submit \
    --master yarn \
    --driver-memory 512m \
    --executor-memory 1g \
    --executor-cores 1 \
    --num-executors 2 \
    spark_user_analysis.py
============================================================
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, sum as spark_sum, avg, max, min,
    to_date, datediff, current_date, when, lit, row_number,
    round as spark_round, broadcast, countDistinct
)
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType, IntegerType

import time

spark = SparkSession.builder \
    .appName("Spark-User-Analysis") \
    .config("spark.sql.adaptive.enabled", "true") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("=" * 60)
print("👤 Spark User Profiling & RFM Analysis")
print("=" * 60)

# ============================================================
# Step 1: Load Data
# ============================================================
print("\n📥 Loading user & order data...")
t0 = time.time()

df_user = spark.sql("SELECT * FROM ods.dim_user")
df_order = spark.sql("SELECT * FROM ods.fact_order") \
    .filter(col("order_status").isin("paid", "shipped", "completed")) \
    .filter(col("actual_amount") > 0) \
    .withColumn("order_date", to_date(col("order_time"))) \
    .withColumn("actual_amount", col("actual_amount").cast(DoubleType()))

print(f"   Users: {df_user.count():,} | Orders: {df_order.count():,}")
print(f"   ⏱  Load: {time.time() - t0:.1f}s")

# ============================================================
# Step 2: User Geo Distribution
# ============================================================
print("\n🌍 [1/5] City-Level User Distribution...")

city_stats = df_user.groupBy("city").agg(
    count("user_id").alias("user_cnt"),
    spark_round(avg("age"), 0).cast(IntegerType()).alias("avg_age"),
    spark_round(avg("vip_level"), 2).alias("avg_vip")
).orderBy(col("user_cnt").desc())

print("   Top 10 Cities by User Count:")
city_stats.show(10, truncate=False)

# ============================================================
# Step 3: VIP Level Consumption Analysis
# ============================================================
print("\n💎 [2/5] VIP Level Analysis...")

# Join user info with orders
user_order = df_order.join(broadcast(df_user), "user_id", "left")

vip_analysis = user_order.groupBy("vip_level").agg(
    countDistinct("user_id").alias("user_cnt"),
    count("order_id").alias("order_cnt"),
    spark_round(spark_sum("actual_amount"), 2).alias("total_revenue"),
    spark_round(avg("actual_amount"), 2).alias("avg_order_value"),
    spark_round(spark_sum("actual_amount") / countDistinct("user_id"), 2).alias("arpu")
).orderBy("vip_level")

print("   VIP Level Consumption Analysis:")
print("   ┌─────────┬──────────┬───────────┬──────────────┬────────────────┬──────────┐")
print("   │ VIP_Lv  │ Users    │ Orders    │ Total Revenue│ Avg Order Val │ ARPU     │")
print("   ├─────────┼──────────┼───────────┼──────────────┼────────────────┼──────────┤")
for row in vip_analysis.collect():
    print(f"   │ {row['vip_level']:<7} │ {row['user_cnt']:<8} │ {row['order_cnt']:<9} │ {row['total_revenue']:<12,.2f} │ {row['avg_order_value']:<14,.2f} │ {row['arpu']:<8,.2f} │")
print("   └─────────┴──────────┴───────────┴──────────────┴────────────────┴──────────┘")

# ============================================================
# Step 4: RFM Fine-Grained Segmentation (9-grid)
# ============================================================
print("\n🎯 [3/5] RFM 9-Grid Segmentation...")

today_date = current_date()

rfm = user_order.groupBy("user_id").agg(
    count("order_id").alias("frequency"),
    spark_round(spark_sum("actual_amount"), 2).alias("monetary"),
    max("order_date").alias("last_order_date")
).withColumn("recency", datediff(lit(today_date), col("last_order_date")))

# 计算分位数阈值（需要转为 Pandas 取分位）
rfm_pd = rfm.select("recency", "frequency", "monetary").toPandas()
r_q33, r_q66 = rfm_pd["recency"].quantile([0.33, 0.66])
f_q33, f_q66 = rfm_pd["frequency"].quantile([0.33, 0.66])
m_q33, m_q66 = rfm_pd["monetary"].quantile([0.33, 0.66])

print(f"   Thresholds: R=[{r_q33:.0f}, {r_q66:.0f}]  F=[{f_q33:.0f}, {f_q66:.0f}]  M=[{m_q33:.0f}, {m_q66:.0f}]")

# R 越小越好，F/M 越大越好
rfm_scored = rfm \
    .withColumn("r_score",
        when(col("recency") <= r_q33, 3)
        .when(col("recency") <= r_q66, 2).otherwise(1)) \
    .withColumn("f_score",
        when(col("frequency") >= f_q66, 3)
        .when(col("frequency") >= f_q33, 2).otherwise(1)) \
    .withColumn("m_score",
        when(col("monetary") >= m_q66, 3)
        .when(col("monetary") >= m_q33, 2).otherwise(1)) \
    .withColumn("rfm_label",
        when((col("r_score") == 3) & (col("f_score") >= 2) & (col("m_score") >= 2), "重要价值客户")
        .when((col("r_score") == 3) & (col("f_score") <= 1) & (col("m_score") >= 2), "重要发展客户")
        .when((col("r_score") <= 2) & (col("f_score") >= 2) & (col("m_score") >= 2), "重要保持客户")
        .when((col("r_score") <= 2) & (col("f_score") <= 1) & (col("m_score") >= 2), "重要挽留客户")
        .when((col("r_score") == 3) & (col("f_score") >= 2) & (col("m_score") <= 1), "一般价值客户")
        .when((col("r_score") <= 2) & (col("f_score") >= 2) & (col("m_score") <= 1), "一般保持客户")
        .when((col("r_score") <= 2) & (col("f_score") <= 1) & (col("m_score") <= 1), "流失客户")
        .otherwise("新客户"))

seg_result = rfm_scored.groupBy("rfm_label").agg(
    count("user_id").alias("user_cnt"),
    spark_round(avg("frequency"), 1).alias("avg_freq"),
    spark_round(avg("monetary"), 2).alias("avg_money"),
    spark_round(avg("recency"), 0).alias("avg_recency_days")
).orderBy(col("user_cnt").desc())

print("\n   RFM Segmentation Result:")
seg_result.show(truncate=False)

# ============================================================
# Step 5: Repurchase Rate Analysis
# ============================================================
print("\n🔄 [4/5] Repurchase Rate Analysis...")

repurchase = rfm \
    .withColumn("purchase_cnt",
        when(col("frequency") == 1, "1次(新客)")
        .when(col("frequency") == 2, "2次")
        .when(col("frequency").between(3, 5), "3-5次")
        .when(col("frequency").between(6, 10), "6-10次")
        .otherwise("10次以上(忠实)")) \
    .groupBy("purchase_cnt").agg(
        count("user_id").alias("user_cnt"),
        spark_round(count("user_id") / lit(rfm.count()) * 100, 1).alias("pct")
    ).orderBy(
        when(col("purchase_cnt") == "1次(新客)", 1)
        .when(col("purchase_cnt") == "2次", 2)
        .when(col("purchase_cnt") == "3-5次", 3)
        .when(col("purchase_cnt") == "6-10次", 4)
        .otherwise(5))

print("   Purchase Frequency Distribution:")
repurchase.show(truncate=False)

# ============================================================
# Step 6: User Lifecycle (Cohort)
# ============================================================
print("\n📅 [5/5] User Registration Cohort Analysis...")

cohort = user_order \
    .withColumn("register_month", col("register_date").substr(1, 7)) \
    .withColumn("order_month", col("order_date").substr(1, 7))

# 按月统计新注册用户的首月购买率
first_order = cohort.groupBy("user_id", "register_month").agg(
    min("order_date").alias("first_order_date")
).withColumn("first_order_month", col("first_order_date").substr(1, 7))

cohort_result = first_order.groupBy("register_month").agg(
    countDistinct("user_id").alias("registered_users"),
    countDistinct(when(
        col("first_order_month") == col("register_month"),
        col("user_id")
    )).alias("first_month_buyers")
).withColumn("conversion_rate",
    spark_round(col("first_month_buyers") / col("registered_users") * 100, 1)
).orderBy("register_month")

print("   Monthly Cohort Conversion Rate:")
cohort_result.show(truncate=False)

# ============================================================
# Save Results to HDFS
# ============================================================
print("\n💾 Saving analysis results...")

output_base = "/data_warehouse/ads/user_analysis"

city_stats.coalesce(1).write.mode("overwrite") \
    .option("header", "true").option("delimiter", "\t") \
    .csv(f"{output_base}/city_distribution")

vip_analysis.coalesce(1).write.mode("overwrite") \
    .option("header", "true").option("delimiter", "\t") \
    .csv(f"{output_base}/vip_analysis")

rfm_scored.coalesce(1).write.mode("overwrite") \
    .option("header", "true").option("delimiter", "\t") \
    .csv(f"{output_base}/rfm_segmentation")

seg_result.coalesce(1).write.mode("overwrite") \
    .option("header", "true").option("delimiter", "\t") \
    .csv(f"{output_base}/rfm_summary")

total_time = time.time() - t0
print(f"\n{'=' * 60}")
print(f"✅ User Analysis Complete — Total: {total_time:.1f}s")
print(f"{'=' * 60}")

spark.stop()
