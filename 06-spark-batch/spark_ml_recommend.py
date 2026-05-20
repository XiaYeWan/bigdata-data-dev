#!/usr/bin/env python3
"""
Spark MLlib 批处理：ALS 协同过滤推荐 + K-Means 用户聚类
============================================================
机器学习应用：
  - ALS (Alternating Least Squares) 商品推荐
  - K-Means 用户分群
  - 评估指标输出 (RMSE, Silhouette)

数据来源：订单明细 → 隐式反馈矩阵 (user_id × product_id × purchase_count)

提交方式:
  /opt/module/spark-3.4.3-bin-hadoop3/bin/spark-submit \
    --master yarn \
    --driver-memory 1g \
    --executor-memory 1g \
    --executor-cores 1 \
    --num-executors 2 \
    spark_ml_recommend.py
============================================================
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, sum as spark_sum, avg, row_number,
    round as spark_round, broadcast, collect_list, explode,
    lit, when
)
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType, IntegerType
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.evaluation import ClusteringEvaluator

import time

spark = SparkSession.builder \
    .appName("Spark-ML-Recommend") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.crossJoin.enabled", "true") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("=" * 60)
print("🤖 Spark MLlib: ALS Recommend + K-Means Cluster")
print("=" * 60)

# ============================================================
# Part A: ALS Collaborative Filtering Recommendation
# ============================================================
print("\n" + "=" * 60)
print("📚 Part A: ALS Product Recommendation")
print("=" * 60)

print("\n[1/5] Loading order detail data...")
t0 = time.time()

# 读取订单明细构建隐式反馈矩阵
try:
    df_detail = spark.sql("SELECT * FROM ods.fact_order_detail")
    has_detail = True
    print(f"   fact_order_detail: {df_detail.count():,} rows")
except Exception:
    has_detail = False
    # 降级：从 fact_order 表构建（但缺少 product_id 粒度）
    print("   ⚠️  fact_order_detail 不可用，尝试从 order + product 关联...")
    df_order = spark.sql("SELECT * FROM ods.fact_order") \
        .filter(col("order_status").isin("paid", "shipped", "completed"))
    print(f"   fact_order: {df_order.count():,} rows")

if has_detail:
    # JOIN fact_order 获取 user_id（detail 表只有 order_id + product_id）
    df_order_for_join = spark.sql("SELECT order_id, user_id FROM ods.fact_order") \
        .filter(col("order_status").isin("paid", "shipped", "completed"))
    df_detail_with_user = df_detail.join(df_order_for_join, "order_id", "left")
    df_detail_with_user = df_detail.join(df_order_for_join, "order_id", "inner")
    df_detail_with_user = df_detail_with_user.filter(col("user_id").isNotNull())
    # 构建评分矩阵：purchase_count 作为隐式反馈强度
    rating_df = df_detail_with_user.groupBy("user_id", "product_id").agg(
        count("detail_id").alias("purchase_count")
    ).withColumn("rating", col("purchase_count").cast(DoubleType()))

    print(f"   User-Product pairs: {rating_df.count():,}")
    print(f"   Unique users: {rating_df.select('user_id').distinct().count():,}")
    print(f"   Unique products: {rating_df.select('product_id').distinct().count():,}")

    # 拆分训练/测试集 (80/20)
    (train, test) = rating_df.randomSplit([0.8, 0.2], seed=42)
    train.cache()
    test.cache()

    print(f"   Train: {train.count():,} | Test: {test.count():,}")

    # ----------------------------------------
    # ALS 模型训练
    # ----------------------------------------
    print("\n[2/5] Training ALS model...")

    als = ALS(
        userCol="user_id",
        itemCol="product_id",
        ratingCol="rating",
        implicitPrefs=False,
        rank=5,
        maxIter=5,
        regParam=0.1,
        coldStartStrategy="drop",
        nonnegative=True,
        seed=42
    )

    model = als.fit(train)
    print("   ✅ ALS model trained")

    # ----------------------------------------
    # 模型评估
    # ----------------------------------------
    print("\n[3/5] Evaluating model...")

    predictions = model.transform(test)
    evaluator = RegressionEvaluator(
        metricName="rmse",
        labelCol="rating",
        predictionCol="prediction"
    )
    rmse = evaluator.evaluate(predictions)
    print(f"   RMSE (Root Mean Square Error): {rmse:.4f}")

    # ----------------------------------------
    # 为每个用户生成 Top-5 推荐
    # ----------------------------------------
    print("\n[4/5] Generating Top-5 recommendations per user...")

    # 获取所有用户
    all_users = rating_df.select("user_id").distinct()

    # 为每个用户推荐 Top 5
    user_recs = model.recommendForUserSubset(all_users, 5)

    # 展开推荐结果
    recs_exploded = user_recs \
        .select("user_id", explode("recommendations").alias("rec")) \
        .select("user_id", col("rec.product_id"), col("rec.rating").alias("score")) \
        .withColumn("score", spark_round(col("score"), 4))

    # 关联商品名称
    product_names = spark.sql("SELECT product_id, product_name, category FROM ods.dim_product")
    final_recs = recs_exploded \
        .join(broadcast(product_names), "product_id", "left") \
        .select("user_id", "product_id", "product_name", "category", "score")

    # 取前 3 个用户的推荐结果展示
    print("\n   Sample Recommendations (First 3 users):")
    top3_users = final_recs.select("user_id").distinct().limit(3)
    sample = final_recs.join(top3_users, "user_id", "inner").orderBy("user_id", col("score").desc())
    sample.show(15, truncate=False)

    # 保存全量推荐结果
    final_recs.coalesce(1).write.mode("overwrite") \
        .option("header", "true").option("delimiter", "\t") \
        .csv("/data_warehouse/ads/ml_recommend/user_top5_recs")

    print(f"   💾 Saved: /data_warehouse/ads/ml_recommend/user_top5_recs/")

else:
    print("   ⚠️  Skipping ALS — fact_order_detail not available, need DataX sync first")

print(f"   ⏱  ALS Part: {time.time() - t0:.1f}s")

# ============================================================
# Part B: K-Means User Clustering
# ============================================================
print("\n" + "=" * 60)
print("🔬 Part B: K-Means User Clustering")
print("=" * 60)

print("\n[5/5] Clustering users by behavior features...")
t1 = time.time()

try:
    # 构建用户行为特征
    df_order_all = spark.sql("""
        SELECT user_id, order_id, order_status, actual_amount, order_time
        FROM ods.fact_order
        WHERE order_status IN ('paid','shipped','completed')
          AND actual_amount > 0
    """)

    # 聚合用户特征
    user_features = df_order_all \
        .withColumn("actual_amount", col("actual_amount").cast(DoubleType())) \
        .groupBy("user_id").agg(
            count("order_id").alias("order_cnt"),
            spark_round(spark_sum("actual_amount"), 2).alias("total_spent"),
            spark_round(avg("actual_amount"), 2).alias("avg_order_value")
        )

    # 特征向量化
    assembler = VectorAssembler(
        inputCols=["order_cnt", "total_spent", "avg_order_value"],
        outputCol="raw_features",
        handleInvalid="skip"
    )
    assembled = assembler.transform(user_features)

    # 标准化
    scaler = StandardScaler(
        inputCol="raw_features",
        outputCol="features",
        withStd=True,
        withMean=True
    )
    scaler_model = scaler.fit(assembled)
    scaled = scaler_model.transform(assembled)

    # K-Means 聚类
    kmeans = KMeans(k=4, seed=42, featuresCol="features", predictionCol="cluster")
    km_model = kmeans.fit(scaled)
    clustered = km_model.transform(scaled)

    # 评估
    evaluator = ClusteringEvaluator(
        featuresCol="features",
        predictionCol="cluster",
        metricName="silhouette"
    )
    silhouette = evaluator.evaluate(clustered)
    print(f"   Silhouette Score: {silhouette:.4f}")

    # 聚类中心
    print("\n   Cluster Centers (scaled):")
    for i, center in enumerate(km_model.clusterCenters()):
        print(f"   Cluster {i}: {center}")

    # 每类用户统计
    print("\n   Cluster Summary:")
    cluster_summary = clustered.groupBy("cluster").agg(
        count("user_id").alias("user_cnt"),
        spark_round(avg("order_cnt"), 1).alias("avg_orders"),
        spark_round(avg("total_spent"), 2).alias("avg_spent"),
        spark_round(avg("avg_order_value"), 2).alias("avg_aov")
    ).orderBy("cluster")

    cluster_summary.show(truncate=False)

    # 保存聚类结果
    clustered.select("user_id", "cluster", "order_cnt", "total_spent", "avg_order_value") \
        .coalesce(1).write.mode("overwrite") \
        .option("header", "true").option("delimiter", "\t") \
        .csv("/data_warehouse/ads/ml_recommend/user_clusters")

    print(f"   💾 Saved: /data_warehouse/ads/ml_recommend/user_clusters/")

except Exception as e:
    print(f"   ⚠️  Clustering skipped: {e}")

print(f"   ⏱  Clustering Part: {time.time() - t1:.1f}s")

# ============================================================
# Summary
# ============================================================
total_time = time.time() - t0
print(f"\n{'=' * 60}")
print(f"✅ ML Recommendation Pipeline Complete — Total: {total_time:.1f}s")
print(f"{'=' * 60}")
print(f"\n📁 Output files:")
print(f"   /data_warehouse/ads/ml_recommend/user_top5_recs/")
print(f"   /data_warehouse/ads/ml_recommend/user_clusters/")
print(f"\n💡 Next: Import into Superset for visualization")

spark.stop()
