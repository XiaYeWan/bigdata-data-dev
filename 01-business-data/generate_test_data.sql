-- ============================================================
-- 电商测试数据生成 (1000+ 用户, 500 商品, 5000 订单)
-- 执行: mysql -uroot -pRoot@123456 < generate_test_data.sql
-- ============================================================

USE ecommerce;

-- 清空旧数据
TRUNCATE fact_order_detail;
TRUNCATE fact_order;
TRUNCATE ods_user_behavior;
TRUNCATE dim_product;
TRUNCATE dim_user;

-- ============================================================
-- 1. 生成 1000 个用户
-- ============================================================
INSERT INTO dim_user (user_id, user_name, gender, age, city, register_date, vip_level)
SELECT
    n,
    CONCAT('user_', LPAD(n, 5, '0')),
    ELT(FLOOR(1 + RAND()*3), 'M', 'F', 'U'),
    FLOOR(18 + RAND() * 42),
    ELT(FLOOR(1 + RAND()*8), '北京', '上海', '广州', '深圳', '杭州', '成都', '武汉', '南京'),
    DATE_SUB('2026-05-01', INTERVAL FLOOR(RAND() * 365) DAY),
    FLOOR(RAND() * 4)
FROM (
    SELECT a.N + b.N * 10 + c.N * 100 + 1 AS n
    FROM (SELECT 0 AS N UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4
          UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) a,
         (SELECT 0 AS N UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4
          UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) b,
         (SELECT 0 AS N UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4
          UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) c
) t
WHERE n <= 1000;

-- ============================================================
-- 2. 生成 500 个商品
-- ============================================================
INSERT INTO dim_product (product_id, product_name, category, brand, price, cost, stock, launch_date)
SELECT
    n,
    CASE FLOOR(1 + RAND()*5)
        WHEN 1 THEN CONCAT('智能手机_', LPAD(n, 4, '0'))
        WHEN 2 THEN CONCAT('时尚T恤_', LPAD(n, 4, '0'))
        WHEN 3 THEN CONCAT('坚果礼盒_', LPAD(n, 4, '0'))
        WHEN 4 THEN CONCAT('精华面霜_', LPAD(n, 4, '0'))
        WHEN 5 THEN CONCAT('智能台灯_', LPAD(n, 4, '0'))
    END,
    ELT(FLOOR(1+RAND()*5), '电子产品', '服装', '食品', '美妆', '家居'),
    ELT(FLOOR(1+RAND()*6), 'Apple', 'Huawei', 'Nike', 'Loreal', '三只松鼠', '小米'),
    ROUND(10 + RAND() * 9990, 2),
    ROUND(5 + RAND() * 5000, 2),
    FLOOR(RAND() * 500),
    DATE_SUB('2026-05-01', INTERVAL FLOOR(RAND() * 180) DAY)
FROM (
    SELECT a.N + b.N * 10 + c.N * 100 + 1 AS n
    FROM (SELECT 0 AS N UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4
          UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) a,
         (SELECT 0 AS N UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4
          UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) b,
         (SELECT 0 AS N UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4
          UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) c
) t
WHERE n <= 500;

-- ============================================================
-- 3. 生成 5000 个订单 + 订单明细
-- ============================================================
-- 使用存储过程批量插入
DELIMITER $$
DROP PROCEDURE IF EXISTS generate_orders$$
CREATE PROCEDURE generate_orders(IN order_count INT)
BEGIN
    DECLARE i INT DEFAULT 0;
    DECLARE v_order_id BIGINT;
    DECLARE v_user_id BIGINT;
    DECLARE v_order_time DATETIME;
    DECLARE v_amount DECIMAL(12,2);
    DECLARE v_discount DECIMAL(10,2);
    DECLARE v_status VARCHAR(20);
    DECLARE v_pay_time DATETIME;
    DECLARE v_item_count INT;
    DECLARE j INT;

    WHILE i < order_count DO
        SET v_order_id = 202605010000 + i;
        SET v_user_id = FLOOR(1 + RAND() * 1000);
        SET v_order_time = DATE_ADD('2026-05-01 00:00:00', INTERVAL FLOOR(RAND() * 17 * 24 * 60) MINUTE);
        SET v_status = ELT(FLOOR(1+RAND()*5), 'pending', 'paid', 'paid', 'shipped', 'completed');
        SET v_pay_time = CASE WHEN v_status IN ('paid','shipped','completed')
                              THEN DATE_ADD(v_order_time, INTERVAL FLOOR(RAND()*60) MINUTE)
                              ELSE NULL END;
        SET v_discount = ROUND(RAND() * 50, 2);

        -- 每单 1-5 个商品
        SET v_item_count = FLOOR(1 + RAND() * 5);
        SET v_amount = 0;
        SET j = 0;
        WHILE j < v_item_count DO
            SET v_amount = v_amount + ROUND(10 + RAND() * 200, 2) * FLOOR(1 + RAND() * 3);
            SET j = j + 1;
        END WHILE;

        INSERT INTO fact_order (order_id, user_id, order_status, order_amount, discount_amount,
                                actual_amount, payment_method, order_time, pay_time)
        VALUES (v_order_id, v_user_id, v_status, v_amount, v_discount,
                v_amount - v_discount, ELT(FLOOR(1+RAND()*3),'alipay','wechat','card'),
                v_order_time, v_pay_time);

        -- 订单明细
        SET j = 0;
        WHILE j < v_item_count DO
            INSERT INTO fact_order_detail (order_id, product_id, quantity, unit_price, subtotal)
            VALUES (v_order_id, FLOOR(1+RAND()*500), FLOOR(1+RAND()*3),
                    ROUND(10+RAND()*200,2), ROUND(10+RAND()*200,2)*FLOOR(1+RAND()*3));
            SET j = j + 1;
        END WHILE;

        SET i = i + 1;
    END WHILE;
END$$
DELIMITER ;

CALL generate_orders(5000);
DROP PROCEDURE IF EXISTS generate_orders;

-- ============================================================
-- 4. 生成 10000 条用户行为日志
-- ============================================================
INSERT INTO ods_user_behavior (user_id, product_id, behavior_type, behavior_time, session_id, ip)
SELECT
    FLOOR(1 + RAND() * 1000),
    FLOOR(1 + RAND() * 500),
    ELT(FLOOR(1+RAND()*4), 'view', 'cart', 'fav', 'buy'),
    DATE_ADD('2026-05-17 00:00:00', INTERVAL FLOOR(RAND() * 24 * 60 * 60) SECOND),
    CONCAT('sess_', LPAD(FLOOR(RAND()*99999), 5, '0')),
    CONCAT('192.168.', FLOOR(1+RAND()*254), '.', FLOOR(1+RAND()*254))
FROM (
    SELECT 1 AS N UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5
    UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9 UNION SELECT 10
) t1,
(   SELECT 1 AS N UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5
    UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9 UNION SELECT 10
) t2,
(   SELECT 1 AS N UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5
    UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9 UNION SELECT 10
) t3,
(   SELECT 1 AS N UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5
    UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9 UNION SELECT 10
) t4
LIMIT 10000;

-- 验证
SELECT '--- 用户表 ---' AS info;  SELECT COUNT(*) AS cnt FROM dim_user;
SELECT '--- 商品表 ---' AS info;  SELECT COUNT(*) AS cnt FROM dim_product;
SELECT '--- 订单表 ---' AS info;  SELECT COUNT(*) AS cnt FROM fact_order;
SELECT '--- 订单明细 ---' AS info;SELECT COUNT(*) AS cnt FROM fact_order_detail;
SELECT '--- 行为日志 ---' AS info;SELECT COUNT(*) AS cnt FROM ods_user_behavior;
