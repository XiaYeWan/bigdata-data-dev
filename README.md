# 📡 Big Data Pipeline — 电商数据采集与实时处理

<p align="center">
  <img src="https://img.shields.io/badge/DataX-全量同步-blue" alt="DataX">
  <img src="https://img.shields.io/badge/Kafka-3.6.1-white?logo=apachekafka" alt="Kafka">
  <img src="https://img.shields.io/badge/Flink-1.17.2-ff69b4?logo=apacheflink" alt="Flink">
  <img src="https://img.shields.io/badge/MySQL-8.0-orange?logo=mysql" alt="MySQL">
  <img src="https://img.shields.io/badge/HDFS-3.3.6-yellow?logo=apachehadoop" alt="HDFS">
  <img src="https://img.shields.io/badge/license-MIT-green" alt="License">
</p>

> 🎯 应届生大数据学习项目：MySQL 业务数据设计 → DataX 批量采集 → Kafka 实时通道 → Flink 流处理。  
> 🔧 与 [bigdata-cluster-deploy](https://github.com/XiaYeWan/bigdata-cluster-deploy) 集群配合使用。  
> 📝 **声明**：本项目为个人学习实践，所有数据均为模拟生成，运行于 VMware 虚拟机环境。

---

## 📖 目录

- [业务场景](#-业务场景)
- [数据架构](#-数据架构)
- [快速开始](#-快速开始)
- [模块说明](#-模块说明)
- [数据验证](#-数据验证)
- [排错实战](#-排错实战-6-个问题--全部解决)
- [项目亮点](#-项目亮点)
- [关联项目](#-关联项目)
- [License](#-license)

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
│   ├── check_data.sh                 # 数据验证巡检
│   └── hive_ods_ddl.sql              # Hive ODS 外表 DDL
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

## 🐛 排错实战 (6 个问题 → 全部解决)

### 🟥 问题 1: DataX MySQL 远程连接被拒

**现象**:
```
ERROR: Access denied for user 'root'@'master' (using password: YES)
```
但 `mysql -uroot -pRoot@123456` 命令行可以连接。

**根因**: MySQL `root` 用户默认只允许 `localhost` 连接，DataX 通过 `jdbc:mysql://master:3306` 连接被视为远程连接。

**解决**:
```sql
GRANT ALL PRIVILEGES ON *.* TO 'root'@'%' IDENTIFIED BY 'Root@123456' WITH GRANT OPTION;
FLUSH PRIVILEGES;
```

> 💡 本地命令能连 ≠ 远程能连，MySQL 用户权限分 `root@localhost` 和 `root@%`。

---

### 🟥 问题 2: DataX HDFS 目标路径不存在

**现象**:
```
ERROR: 您配置的path: [/user/hive/warehouse/ods.db/dim_user] 不存在
```

**根因**: DataX HdfsWriter 要求目标目录必须预先存在，Hive `CREATE TABLE` 只是写元数据，不自动建 HDFS 目录。

**解决**:
```bash
hdfs dfs -mkdir -p /user/hive/warehouse/ods.db/dim_user
hdfs dfs -mkdir -p /user/hive/warehouse/ods.db/dim_product
hdfs dfs -mkdir -p /user/hive/warehouse/ods.db/fact_order
```

> 💡 用 DataX 写 HDFS 前，先 `hdfs dfs -ls` 确认路径存在。

---

### 🟥 问题 3: Hive 命令找不到

**现象**: `bash: hive: 未找到命令...`

**根因**: `PATH` 环境变量没有包含 Hive 的 `bin` 目录。

**解决**: 使用完整路径 `/opt/module/hive-3.1.3/bin/hive`

**永久方案**:
```bash
echo 'export PATH=$PATH:/opt/module/hive-3.1.3/bin' >> ~/.bashrc
source ~/.bashrc
```

> 💡 自己安装的组件 PATH 不一定自动生效，先 `which hive` 确认。

---

### 🟥 问题 4: Kafka 连接 ZooKeeper 超时

**现象**: `ZooKeeperClientTimeoutException` — ZK 进程正常但 Kafka 跨节点连不上。

**根因**: Slave 节点防火墙未完全关闭，2181 端口被 `iptables` 规则拦截。

**排查**:
```bash
nc slave1 2181          # 无响应 → 端口被封
iptables -L -n          # 发现拦截规则
```

**解决**:
```bash
for host in slave1 slave2; do
    ssh $host "systemctl stop firewalld; systemctl disable firewalld; iptables -F"
done
```

> 💡 集群组件连接超时，**先查防火墙**。`firewalld` 和 `iptables` 是两个层面。

---

### 🟥 问题 5: pip3 安装 kafka-python 报错

**现象**: `pip3 install kafka-python` 报 `No matching distribution found`

**根因**: CentOS 7 自带的 Python 3.6 + pip3 版本过老，部分包索引失效。

**解决**:
```bash
# 升级 pip 后再装
pip3 install --upgrade pip
pip3 install kafka-python
```

> 💡 EOL 系统上的 pip 也需要先升级。

---

### 🟥 问题 6: DataX JSON 配置中 JDBC 连接串写法

**现象**: DataX 报 `Communications link failure`

**根因**: `jdbc:mysql://localhost:3306/ecommerce` 在 DataX 运行时会走 TCP 连接，不能用 `localhost`（DataX 可能不在同一主机上运行）。

**解决**: 明确写主机名或 IP：
```json
"jdbcUrl": "jdbc:mysql://master:3306/ecommerce?useSSL=false&serverTimezone=Asia/Shanghai"
```

> 💡 DataX Reader 配置中 `localhost` 只在 DataX 与 MySQL 同机时有效，生产一律用主机名。

---

## 💡 项目亮点

| 技术领域 | 实践内容 |
|----------|----------|
| 📊 **业务建模** | 5 表电商场景设计，维度表 + 事实表，存储过程批量造数 |
| 🔄 **批量采集** | DataX MySQL → HDFS，3 通道并行，JSON 配置化 |
| 📨 **实时通道** | Kafka 3 Broker 集群，2 Topic，Python 模拟实时生产者 |
| 🌊 **流处理** | Flink SQL 窗口聚合，事件时间 Watermark，Kafka → HDFS |
| 🤖 **运维自动化** | 一键启动脚本编排 4 个步骤，全流程可视化输出 |

---

## 📝 学习笔记：数仓分层 vs 机器学习预处理

在学习过程中，我发现数仓 ETL 的四层架构和机器学习数据预处理有很多相似之处：

| 对比维度 | 机器学习预处理 | 数仓分层开发 |
|----------|---------------|-------------|
| 原始数据 | `raw.csv` | ODS 层（贴源数据） |
| 清洗去重 | `drop_duplicates / fillna` | DWD 层（Hive SQL 去重/去空/标准化） |
| 特征聚合 | `groupby / merge / pivot` | DWS 层（聚合宽表） |
| 最终输出 | `X_train, y_train` | ADS 层（业务指标） |
| 工具 | pandas（MB 级） | Hive/Spark（TB 级） |
| 验证方式 | `print(df.shape)` | `SELECT COUNT(*)` + 对账脚本 |

**核心思想一致**：脏数据进来 → 洗干净 → 聚合 → 输出可用数据。区别在于规模、工具和工程化程度。

---

## 🔗 关联项目

| 项目 | 说明 |
|------|------|
| [bigdata-cluster-deploy](https://github.com/XiaYeWan/bigdata-cluster-deploy) | 大数据集群搭建（基础设施层，Day1） |
| [bigdata-data-warehouse](https://github.com/XiaYeWan/bigdata-data-warehouse) | 离线数仓ETL + DS调度 + Superset可视化（Day3） |

---

## 📄 License

MIT © 2026 BigData-Dev Contributors

---

> 📅 **创建日期**: 2026-05-18  
> ⭐ **项目状态**: 数据采集 ✅ | 实时管道 ✅ | 已完成
