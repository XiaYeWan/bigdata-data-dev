-- ============================================================
-- Hive ODS 层建外表 (映射 HDFS 上的 DataX 同步数据)
-- 执行: /opt/module/hive-3.1.3/bin/hive -f hive_ods_ddl.sql
-- ============================================================

-- 创建 ODS 库
CREATE DATABASE IF NOT EXISTS ods
COMMENT '贴源数据层 — 与MySQL业务表1:1映射'
LOCATION '/data_warehouse/ods/';

USE ods;

-- 1. 用户维度表
DROP TABLE IF EXISTS ods.dim_user;
CREATE EXTERNAL TABLE ods.dim_user (
    user_id        BIGINT   COMMENT '用户ID',
    user_name      STRING   COMMENT '用户名',
    gender         STRING   COMMENT '性别(M/F/U)',
    age            INT      COMMENT '年龄',
    city           STRING   COMMENT '城市',
    register_date  STRING   COMMENT '注册日期',
    vip_level      INT      COMMENT '会员等级',
    create_time    STRING   COMMENT '创建时间'
)
COMMENT '用户维度表 (ODS贴源层)'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/data_warehouse/ods/user/full/2026-05-18';

-- 2. 商品维度表
DROP TABLE IF EXISTS ods.dim_product;
CREATE EXTERNAL TABLE ods.dim_product (
    product_id    BIGINT    COMMENT '商品ID',
    product_name  STRING    COMMENT '商品名称',
    category      STRING    COMMENT '品类',
    brand         STRING    COMMENT '品牌',
    price         DOUBLE    COMMENT '单价',
    cost          DOUBLE    COMMENT '成本',
    stock         INT       COMMENT '库存',
    launch_date   STRING    COMMENT '上架日期',
    create_time   STRING    COMMENT '创建时间'
)
COMMENT '商品维度表 (ODS贴源层)'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/data_warehouse/ods/product/full/2026-05-18';

-- 3. 订单事实表
DROP TABLE IF EXISTS ods.fact_order;
CREATE EXTERNAL TABLE ods.fact_order (
    order_id       BIGINT    COMMENT '订单ID',
    user_id        BIGINT    COMMENT '用户ID',
    order_status   STRING    COMMENT '订单状态',
    order_amount   DOUBLE    COMMENT '订单金额',
    discount_amount DOUBLE   COMMENT '优惠金额',
    actual_amount  DOUBLE    COMMENT '实付金额',
    payment_method STRING    COMMENT '支付方式',
    order_time     STRING    COMMENT '下单时间',
    pay_time       STRING    COMMENT '支付时间',
    create_time    STRING    COMMENT '创建时间',
    update_time    STRING    COMMENT '更新时间'
)
COMMENT '订单事实表 (ODS贴源层)'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/data_warehouse/ods/orders/full/2026-05-18';

-- ============================================================
-- 验证
-- ============================================================
SELECT '=== ODS 表列表 ===' AS info;
SHOW TABLES IN ods;

SELECT '=== dim_user 数据量 ===' AS info;
SELECT COUNT(*) AS row_cnt FROM ods.dim_user;

SELECT '=== dim_product 数据量 ===' AS info;
SELECT COUNT(*) AS row_cnt FROM ods.dim_product;

SELECT '=== fact_order 数据量 ===' AS info;
SELECT COUNT(*) AS row_cnt FROM ods.fact_order;

SELECT '=== 订单金额统计 ===' AS info;
SELECT
    COUNT(*)                    AS order_cnt,
    ROUND(SUM(actual_amount),2) AS total_amount,
    ROUND(AVG(actual_amount),2) AS avg_amount,
    ROUND(MAX(actual_amount),2) AS max_amount
FROM ods.fact_order
WHERE order_status IN ('paid','shipped','completed');

SELECT '=== 订单状态分布 ===' AS info;
SELECT
    order_status,
    COUNT(*) AS cnt
FROM ods.fact_order
GROUP BY order_status
ORDER BY cnt DESC;
