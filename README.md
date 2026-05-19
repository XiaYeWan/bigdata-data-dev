# 📡 Big Data Pipeline — 电商数据采集与实时处理

<p align="center">
  <img src="https://img.shields.io/badge/DataX-全量同步-blue" alt="DataX">
  <img src="https://img.shields.io/badge/Kafka-3.6.1-white?logo=apachekafka" alt="Kafka">
  <img src="https://img.shields.io/badge/Flink-1.17.2-ff69b4?logo=apacheflink" alt="Flink">
  <img src="https://img.shields.io/badge/MySQL-8.0-orange?logo=mysql" alt="MySQL">
  <img src="https://img.shields.io/badge/HDFS-3.3.6-yellow?logo=apachehadoop" alt="HDFS">
  <img src="https://img.shields.io/badge/license-MIT-green" alt="License">
</p>

> 🎯 基于电商场景的数据开发项目：MySQL 业务数据设计 → DataX 批量采集 → Kafka 实时通道 → Flink 流处理。  
> 🔧 与 [bigdata-cluster-deploy](https://github.com/XiaYeWan/bigdata-cluster-deploy) 集群配合使用。

---

## 📖 目录

- [企业数据开发全景](#-企业数据开发全景)
- [业务场景](#-业务场景)
- [数据架构](#-数据架构)
- [快速开始](#-快速开始)
- [模块说明](#-模块说明)
- [数据验证](#-数据验证)
- [项目亮点](#-项目亮点)

---

## 🏢 企业数据开发全景

> 本项目的核心不是「搭通道」，而是展示数据开发岗位在企业中的真实工作模式。

### 数据开发工程师日常

```
一次取数(15%)  → SELECT + JOIN + GROUP BY, 临时分析需求
ETL开发(40%)   → Hive SQL 清洗/汇总/宽表，从ODS写到ADS
数据质量(15%)  → DQC规则配置、脏数据隔离、对账脚本
任务调度(15%)  → DolphinScheduler 编排、依赖链、失败重试
问题排查(15%)  → 数据延迟、分区遗漏、上游变更、Spark OOM
```

### 数据开发 vs 机器学习预处理

| 对比维度 | 机器学习预处理 | 数仓分层开发 |
|----------|---------------|-------------|
| 原始数据 | `raw.csv` | ODS 层（贴源数据） |
| 清洗去重 | `drop_duplicates / fillna` | DWD 层（Hive SQL 去重/去空/标准化） |
| 特征工程 | `groupby / merge / pivot` | DWS 层（聚合宽表） |
| 最终输出 | `X_train, y_train` | ADS 层（业务指标） |
| 工具 | pandas（MB 级） | Hive/Spark（TB 级） |
| 验证方式 | `print(df.shape)` | `SELECT COUNT(*)` + 对账脚本 |
| 编排方式 | Python 脚本 | DolphinScheduler 定时 DAG |
| 数据质量 | 人工检查 | DQC 自动化规则 |

**核心思想一致**：脏数据进来 → 洗干净 → 聚合 → 输出可用数据。区别在于规模、工具和工程化程度。

### 本项目覆盖的数据开发环节

| 环节 | 状态 | 技术 | 产出 |
|------|:--:|------|------|
| 业务数据建模 | ✅ | MySQL DDL | 5 张表，维度表 + 事实表 |
| 离线批量采集 | ✅ | DataX | MySQL → HDFS ODS，3 张表 |
| 实时数据通道 | ✅ | Kafka | 2 Topic，Python 模拟 Producer |
| Hive 外表映射 | ✅ | Hive DDL | ODS 层对 HDFS 数据建外表 |
| 流处理 | ✅ | Flink SQL | Kafka → 窗口聚合 → HDFS |
| ODS→DWD 清洗 | 🔜 下一项目 | Hive SQL | 去重/去空/标准化 |
| DWD→DWS 汇总 | 🔜 下一项目 | Hive SQL | 跨表关联，宽表 |
| ADS 指标 | 🔜 下一项目 | Hive SQL | 面向报表的年月日指标 |
| 任务调度 | 🔜 下一项目 | DolphinScheduler | 定时 DAG 编排 |

---

## 🏪 业务场景

模拟电商平台，包含 **5 张业务表** 和 **2 条数据管道**：

| 数据表 | 类型 | 数据量 | 说明 |
|--------|------|--------|------|
| `dim_user` | 维度表 | 1,000 | 用户信息（注册日期/会员等级/城市） |
| `dim_product` | 维度表 | 500 | 商品信息（品类/品牌/价格/成本） |
| `fact_order` | 事实表 | 5,000 | 订单主表（金额/状态/支付方式） |
| `fact_order_detail` | 事实表 | ~15,000 | 订单明细（商品/数量/单价） |
| `ods_user_behavior` | 日志表 | 10,000 | 用户行为（浏览/加购/收藏/购买） |

---

## 🏗️ 数据架构

```
┌──────────────┐   全量同步(DataX)    ┌─────────────────┐
│   MySQL      │ ──────────────────→  │  HDFS /data_     │
│  ecommerce   │    dim_user          │  warehouse/ods/  │
│  5 张业务表   │    dim_product       │    user/product/  │
│              │    fact_order        │    orders/       │
└──────┬───────┘                      └─────────────────┘
       │                                       ↑
       │ 实时采集(Kafka)                        │ 流处理(Flink)
       ▼                                       │
┌──────────────┐    ┌──────────────────┐    ┌──┴──────────┐
│ user-behavior│ →  │  Flink SQL       │ →  │ HDFS 实时    │
│ -log Topic   │    │  窗口聚合/统计    │    │ 聚合结果     │
├──────────────┤    └──────────────────┘    └─────────────┘
│ order-stream │
│ Topic        │
└──────────────┘
```

---

## 🚀 快速开始

### 前置条件

- 大数据集群已启动（参考 [bigdata-cluster-deploy](https://github.com/XiaYeWan/bigdata-cluster-deploy)）
- 各节点已关闭防火墙
- Python 3 环境 + `kafka-python` 已安装

```bash
# 安装 Python 依赖
pip3 install kafka-python
```

### 一键启动

```bash
cd /root/bigdata-data-dev
bash 05-scripts/start_data_pipeline.sh
```

### 分步执行

```bash
# 1. MySQL 建表 + 造数据
mysql -uroot -pRoot@123456 < 01-business-data/mysql_business_tables.sql
mysql -uroot -pRoot@123456 < 01-business-data/generate_test_data.sql

# 2. DataX 全量同步
python3 /opt/module/datax/bin/datax.py 02-datax-sync/mysql_to_hdfs_user.json
python3 /opt/module/datax/bin/datax.py 02-datax-sync/mysql_to_hdfs_product.json
python3 /opt/module/datax/bin/datax.py 02-datax-sync/mysql_to_hdfs_orders.json

# 3. 创建 Kafka Topic
bash 03-kafka-setup/create_topics.sh

# 4. 启动实时数据生产者(后台)
nohup python3 03-kafka-setup/kafka_producer.py --topic user-behavior-log --rate 5 &
nohup python3 03-kafka-setup/kafka_producer.py --topic order-stream --rate 3 &
```

---

## 📁 模块说明

```
bigdata-data-dev/
├── 01-business-data/
│   ├── mysql_business_tables.sql     # 5 张业务表 DDL
│   └── generate_test_data.sql        # 存储过程造测试数据
├── 02-datax-sync/
│   ├── mysql_to_hdfs_user.json       # DataX: 用户表 → HDFS
│   ├── mysql_to_hdfs_product.json    # DataX: 商品表 → HDFS
│   └── mysql_to_hdfs_orders.json     # DataX: 订单表 → HDFS
├── 03-kafka-setup/
│   ├── create_topics.sh              # 创建 Kafka Topic
│   └── kafka_producer.py             # 模拟实时数据生产
├── 04-flink-stream/
│   └── flink_sql_job.sql             # Flink SQL 流处理
├── 05-scripts/
│   ├── start_data_pipeline.sh        # 一键启动数据管道
│   └── check_data.sh                 # 数据验证巡检
├── .gitignore
├── LICENSE
└── README.md
```

---

## ✅ 数据验证

```bash
bash 05-scripts/check_data.sh
```

检查以下维度：

| 验证项 | 预期结果 |
|--------|----------|
| MySQL 数据量 | 5 表共 31,500+ 条记录 |
| HDFS ODS 文件 | `/data_warehouse/ods/` 下有 3 个表目录 |
| Kafka Topic 状态 | `user-behavior-log` + `order-stream` 已创建 |
| 实时数据流 | 持续有 JSON 消息产出 |

---

## 💡 项目亮点

| 技术领域 | 实践内容 |
|----------|----------|
| 📊 **业务建模** | 5 表电商场景设计，维度表 + 事实表数仓建模前置 |
| 🔄 **批量采集** | DataX MySQL → HDFS，3 通道并行，JSON 配置化 |
| 📨 **实时通道** | Kafka 3 Broker 集群，2 Topic，Python 模拟实时生产者 |
| 🌊 **流处理** | Flink SQL 窗口聚合，事件时间 Watermark，Kafka → HDFS |
| 🤖 **运维自动化** | 一键启动脚本编排 4 个步骤，全流程可视化输出 |

---

## 🔗 关联项目

| 项目 | 说明 |
|------|------|
| [bigdata-cluster-deploy](https://github.com/XiaYeWan/bigdata-cluster-deploy) | 大数据集群搭建（基础设施层） |
| 数仓建设 (规划中) | Hive 分层建模 ODS→DWD→DWS→ADS |
| 可视化 (规划中) | Superset 看板 + DolphinScheduler 调度 |

---

## 📄 License

MIT © 2026 BigData-Dev Contributors

---

> 📅 **创建日期**: 2026-05-18  
> ⭐ **项目状态**: 🚧 进行中（离线采集 ✅ ｜ 实时管道 🚧）
