#!/usr/bin/env python3
"""
Spark 批处理：商品销售分析 & 关联规则挖掘
============================================================
分析维度：
  - 品类销售排行 (GMV / 销量 / 利润率)
  - 品牌 Top N 排行
  - 商品关联 (购物篮分析 - 同单商品 co-occurrence)
  - 高利润单品识别

提交方式:
  /opt/module/spark-3.4.3-bin-hadoop3/bin/spark-submit \
    --master yarn \
    --driver-memory 512m \
    --executor-memory 1g \
    --executor-cores 1 \
    --num-executors 2 \
    spark_product_analysis.py
============================================================
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, sum as spark_sum, avg, max, min, row_number,
    round as spark_round, broadcast, collect_list, explode, size,
    array_contains, concat_ws, countDistinct, when, lit
)
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType

import time

spark = SparkSession.builder \
    .appName("Spark-Product-Analysis") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.crossJoin.enabled", "true") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("=" * 60)
print("🛒 Spark Product Sales & Association Analysis")
print("=" * 60)

# ============================================================
# Step 1: Load Data
# ============================================================
print("\n📥 Loading product & order data...")
t0 = time.time()

df_product = spark.sql("SELECT * FROM ods.dim_product") \
    .withColumn("price", col("price").cast(DoubleType())) \
    .withColumn("cost", col("cost").cast(DoubleType()))

# 注意: fact_order_detail 可能在 Hive 中有，否则从 MySQL 直读
# 这里尝试从 Hive 读取，如果不存在则从订单表聚合分析
try:
    df_detail = spark.sql("SELECT * FROM ods.fact_order_detail")
    has_detail = True
except Exception:
    has_detail = False
    # 没明细表就用订单表做品类分析（通过 product_id 关联）
    df_order = spark.sql("SELECT * FROM ods.fact_order") \
        .filter(col("order_status").isin("paid", "shipped", "completed")) \
        .filter(col("actual_amount") > 0)

print(f"   Products: {df_product.count():,}")

if has_detail:
    print(f"   Order Details: {df_detail.count():,}")
    df_analysis = df_detail \
        .join(broadcast(df_product), "product_id", "left")
else:
    print("   ⚠️  fact_order_detail 不可用，使用品类级别聚合分析")
    # 品类分析走另一条路
    print(f"   Orders: {df_order.count():,}")

print(f"   ⏱  Load: {time.time() - t0:.1f}s")

# ============================================================
# Step 2: Category Sales Ranking
# ============================================================
print("\n📊 [1/5] Category Sales Ranking...")

if has_detail:
    cat_analysis = df_analysis.groupBy("category").agg(
        countDistinct("product_id").alias("product_cnt"),
        count("detail_id").alias("sold_qty"),
        spark_round(spark_sum("subtotal"), 2).alias("total_gmv"),
        spark_round(spark_sum(col("subtotal") - col("quantity") * col("cost")), 2).alias("total_profit"),
        spark_round(
            spark_sum(col("subtotal") - col("quantity") * col("cost")) /
            spark_sum("subtotal") * 100, 1
        ).alias("profit_margin_pct")
    ).orderBy(col("total_gmv").desc())
else:
    # 无明细时按商品维度做聚合（品类从 product 表来）
    product_order = df_order.join(broadcast(df_product), "product_id", "left") if "product_id" in df_order.columns else None
    if product_order is not None:
        cat_analysis = product_order.groupBy("category").agg(
            countDistinct("product_id").alias("product_cnt"),
            count("order_id").alias("order_cnt"),
            spark_round(spark_sum("actual_amount"), 2).alias("total_gmv")
        ).orderBy(col("total_gmv").desc())
    else:
        # 最简单的：按品类统计商品
        cat_analysis = df_product.groupBy("category").agg(
            count("product_id").alias("product_cnt"),
            spark_round(avg("price"), 2).alias("avg_price"),
            spark_round(avg(col("price") - col("cost")), 2).alias("avg_profit_per_item")
        ).orderBy(col("product_cnt").desc())

print("   Category Analysis:")
cat_analysis.show(truncate=False)

# ============================================================
# Step 3: Brand Top-N Ranking
# ============================================================
print("\n🏷️  [2/5] Brand Top-N Analysis...")

brand_window = Window.partitionBy("category").orderBy(col("brand_sales").desc())

if has_detail:
    brand_analysis = df_analysis.groupBy("category", "brand").agg(
        spark_round(spark_sum("subtotal"), 2).alias("brand_sales"),
        countDistinct("product_id").alias("brand_products")
    ).withColumn("brand_rank", row_number().over(brand_window)) \
     .filter(col("brand_rank") <= 3) \
     .orderBy("category", "brand_rank")

    print("   Top 3 Brands per Category:")
    brand_analysis.show(20, truncate=False)
else:
    brand_analysis = df_product.groupBy("category", "brand").agg(
        count("product_id").alias("product_cnt"),
        spark_round(avg("price"), 2).alias("avg_price")
    ).orderBy("category", col("product_cnt").desc())

    print("   Brand Product Count by Category:")
    brand_analysis.show(20, truncate=False)

# ============================================================
# Step 4: Product Association (Basket Analysis)
# ============================================================
print("\n🔗 [3/5] Product Co-Purchase Association...")

if has_detail:
    # 收集每个订单中的商品集合
    order_basket = df_detail.groupBy("order_id").agg(
        collect_list("product_id").alias("products")
    ).filter(size(col("products")) >= 2)

    print(f"   Multi-product orders: {order_basket.count():,}")

    # 展开商品对 (Self-join on order_id)
    pairs = df_detail.alias("a").join(
        df_detail.alias("b"),
        (col("a.order_id") == col("b.order_id")) &
        (col("a.product_id") < col("b.product_id")),
        "inner"
    ).select(
        col("a.product_id").alias("product_a"),
        col("b.product_id").alias("product_b")
    )

    # 统计共现次数
    co_occur = pairs.groupBy("product_a", "product_b").agg(
        count("*").alias("co_count")
    ).orderBy(col("co_count").desc())

    # 关联商品名称
    product_names = df_product.select(
        col("product_id").alias("pid"), col("product_name")
    )

    top_pairs = co_occur.limit(10) \
        .join(product_names, col("product_a") == col("pid"), "left") \
        .drop("pid") \
        .withColumnRenamed("product_name", "name_a") \
        .join(product_names, col("product_b") == col("pid"), "left") \
        .drop("pid") \
        .withColumnRenamed("product_name", "name_b") \
        .select("name_a", "name_b", "co_count")

    print("   Top 10 Co-Purchased Product Pairs:")
    top_pairs.show(10, truncate=False)

    # 保存共现结果
    co_occur.coalesce(1).write.mode("overwrite") \
        .option("header", "true").option("delimiter", "\t") \
        .csv("/data_warehouse/ads/product_analysis/co_occurrence")

# ============================================================
# Step 5: High-Profit Product Identification
# ============================================================
print("\n💰 [4/5] High-Profit Product Identification...")

profit_window = Window.orderBy(col("profit_per_item").desc())

profit_products = df_product \
    .withColumn("margin", spark_round((col("price") - col("cost")) / col("price") * 100, 1)) \
    .withColumn("profit_per_item", spark_round(col("price") - col("cost"), 2)) \
    .withColumn("profit_rank", row_number().over(profit_window)) \
    .filter(col("profit_rank") <= 20) \
    .select("profit_rank", "product_name", "category", "brand",
            "price", "cost", "profit_per_item", "margin") \
    .orderBy("profit_rank")

print("   Top 20 High-Margin Products:")
profit_products.show(20, truncate=False)

# ============================================================
# Step 6: Category Price Distribution
# ============================================================
print("\n📉 [5/5] Price Distribution by Category...")

price_dist = df_product.groupBy("category").agg(
    count("product_id").alias("product_cnt"),
    spark_round(min("price"), 2).alias("min_price"),
    spark_round(avg("price"), 2).alias("avg_price"),
    spark_round(max("price"), 2).alias("max_price"),
    spark_round(avg(col("price") - col("cost")), 2).alias("avg_margin")
).orderBy(col("product_cnt").desc())

print("   Price Distribution by Category:")
price_dist.show(truncate=False)

# ============================================================
# Save Results
# ============================================================
print("\n💾 Saving product analysis results...")

output_base = "/data_warehouse/ads/product_analysis"

cat_analysis.coalesce(1).write.mode("overwrite") \
    .option("header", "true").option("delimiter", "\t") \
    .csv(f"{output_base}/category_ranking")

profit_products.coalesce(1).write.mode("overwrite") \
    .option("header", "true").option("delimiter", "\t") \
    .csv(f"{output_base}/top_margin_products")

price_dist.coalesce(1).write.mode("overwrite") \
    .option("header", "true").option("delimiter", "\t") \
    .csv(f"{output_base}/price_distribution")

total_time = time.time() - t0
print(f"\n{'=' * 60}")
print(f"✅ Product Analysis Complete — Total: {total_time:.1f}s")
print(f"{'=' * 60}")

spark.stop()
