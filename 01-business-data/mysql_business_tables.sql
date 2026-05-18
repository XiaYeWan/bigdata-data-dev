-- ============================================================
-- 电商业务表设计
-- 执行: mysql -uroot -pRoot@123456 < mysql_business_tables.sql
-- ============================================================

CREATE DATABASE IF NOT EXISTS ecommerce DEFAULT CHARSET utf8mb4 COLLATE utf8mb4_unicode_ci;
USE ecommerce;

-- 1. 用户表
DROP TABLE IF EXISTS dim_user;
CREATE TABLE dim_user (
    user_id       BIGINT PRIMARY KEY COMMENT '用户ID',
    user_name     VARCHAR(50)  NOT NULL COMMENT '用户名',
    gender        VARCHAR(2)   COMMENT '性别(M/F/U)',
    age           INT          COMMENT '年龄',
    city          VARCHAR(50)  COMMENT '城市',
    register_date DATE         NOT NULL COMMENT '注册日期',
    vip_level     INT DEFAULT 0 COMMENT '会员等级 0-3',
    create_time   DATETIME DEFAULT CURRENT_TIMESTAMP,
    update_time   DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='用户维度表';

-- 2. 商品表
DROP TABLE IF EXISTS dim_product;
CREATE TABLE dim_product (
    product_id    BIGINT PRIMARY KEY COMMENT '商品ID',
    product_name  VARCHAR(200) NOT NULL COMMENT '商品名称',
    category      VARCHAR(50)  NOT NULL COMMENT '品类(电子产品/服装/食品/美妆/家居)',
    brand         VARCHAR(50)  COMMENT '品牌',
    price         DECIMAL(10,2) NOT NULL COMMENT '单价',
    cost          DECIMAL(10,2) NOT NULL COMMENT '成本',
    stock         INT DEFAULT 0 COMMENT '库存',
    launch_date   DATE COMMENT '上架日期',
    create_time   DATETIME DEFAULT CURRENT_TIMESTAMP,
    update_time   DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='商品维度表';

-- 3. 订单表
DROP TABLE IF EXISTS fact_order;
CREATE TABLE fact_order (
    order_id      BIGINT PRIMARY KEY COMMENT '订单ID',
    user_id       BIGINT NOT NULL COMMENT '用户ID',
    order_status  VARCHAR(20) NOT NULL DEFAULT 'pending' COMMENT 'pending/paid/shipped/completed/cancelled',
    order_amount  DECIMAL(12,2) NOT NULL COMMENT '订单金额',
    discount_amount DECIMAL(10,2) DEFAULT 0 COMMENT '优惠金额',
    actual_amount DECIMAL(12,2) NOT NULL COMMENT '实付金额',
    payment_method VARCHAR(20) COMMENT '支付方式(alipay/wechat/card)',
    order_time    DATETIME NOT NULL COMMENT '下单时间',
    pay_time      DATETIME COMMENT '支付时间',
    create_time   DATETIME DEFAULT CURRENT_TIMESTAMP,
    update_time   DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='订单事实表';

-- 4. 订单明细表
DROP TABLE IF EXISTS fact_order_detail;
CREATE TABLE fact_order_detail (
    detail_id     BIGINT PRIMARY KEY AUTO_INCREMENT COMMENT '明细ID',
    order_id      BIGINT NOT NULL COMMENT '订单ID',
    product_id    BIGINT NOT NULL COMMENT '商品ID',
    quantity      INT NOT NULL COMMENT '购买数量',
    unit_price    DECIMAL(10,2) NOT NULL COMMENT '成交单价',
    subtotal      DECIMAL(12,2) NOT NULL COMMENT '小计',
    create_time   DATETIME DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='订单明细事实表';

-- 5. 用户行为日志表 (模拟实时采集源)
DROP TABLE IF EXISTS ods_user_behavior;
CREATE TABLE ods_user_behavior (
    log_id        BIGINT PRIMARY KEY AUTO_INCREMENT COMMENT '日志ID',
    user_id       BIGINT NOT NULL COMMENT '用户ID',
    product_id    BIGINT COMMENT '商品ID',
    behavior_type VARCHAR(20) NOT NULL COMMENT '行为类型(view/cart/fav/buy)',
    behavior_time DATETIME NOT NULL COMMENT '行为时间',
    session_id    VARCHAR(50) COMMENT '会话ID',
    ip            VARCHAR(20) COMMENT 'IP地址',
    create_time   DATETIME DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='用户行为日志表';

-- 创建 DataX 同步专用用户
CREATE USER IF NOT EXISTS 'datax'@'%' IDENTIFIED WITH mysql_native_password BY 'Datax@123456';
GRANT SELECT,RELOAD,LOCK TABLES,REPLICATION CLIENT ON *.* TO 'datax'@'%';
GRANT SELECT ON ecommerce.* TO 'datax'@'%';
FLUSH PRIVILEGES;

SELECT '>>> MySQL 业务表创建完成' AS status;
