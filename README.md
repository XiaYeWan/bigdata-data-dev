# 📡 Big Data Pipeline — 电商数据采集与实时处理

<p align="center">
  <img src="https://img.shields.io/badge/DataX-全量同步-blue" alt="DataX">
  <img src="https://img.shields.io/badge/Kafka-3.6.1-white?logo=apachekafka" alt="Kafka">
  <img src="https://img.shields.io/badge/Flink-1.17.2-ff69b4?logo=apacheflink" alt="Flink">
  <img src="https://img.shields.io/badge/MySQL-8.0-orange?logo=mysql" alt="MySQL">
  <img src="https://img.shields.io/badge/HDFS-3.3.6-yellow?logo=apachehadoop" alt="HDFS">
  <img src="https://img.shields.io/badge/license-MIT-green" alt="License">
</p>

> 🎯 个人学习实践：MySQL 业务数据设计 → DataX 批量采集 → Kafka 实时通道 → Flink 流处理。  
> 🔧 与 [bigdata-cluster-deploy](https://github.com/XiaYeWan/bigdata-cluster-deploy) 集群配合使用。  
> 📝 **声明**：本项目为个人学习实践，所有数据均为模拟生成，运行于 VMware 虚拟机环境。

---

## 📖 目录

- [业务场景](#-业务场景)
- [数据架构](#-数据架构)
- [快速开始](#-快速开始)
- [模块说明](#-模块说明)
- [数据验证](#-数据验证)
- [排错实战](#-排错实战-11-个问题--全部解决)
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
│   ├── flink_sql_job.sql              # Flink SQL 流处理（已验证）
│   ├── flink-conf-standalone.yaml     # Flink 集群参考配置
│   └── realtime_consumer.py           # Python 消费者备用方案
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
| 实时数据流 | Python Producer 持续推送 JSON 消息 |
| Flink SQL 窗口聚合 | 每分钟输出 `buy/cart/view/fav` 四类 UV/PV |

---

## 🐛 排错实战 (11 个问题 → 全部解决)

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

### 🟥 问题 7: Flink 集群内存不足无法启动

**现象**: `IllegalConfigurationException: Sum of configured JVM Metaspace and JVM Overhead exceed configured Total Process Memory`

**根因**: 7.6G 物理机上同时跑 HDFS + YARN + Kafka + ZK，默认 Flink 内存配得太大（1.6G+）。

**解决**: 精简配置，停掉非必要进程（DolphinScheduler 4 进程占了 ~2.3G）：
```bash
# flink-conf.yaml
jobmanager.memory.process.size: 800m
taskmanager.memory.process.size: 1200m
parallelism.default: 1
```

> 💡 虚拟机学习环境要先 `free -h` 算可用内存，再配 Flink 内存。

---

### 🟥 问题 8: Flink SQL Client `Connection refused: localhost/127.0.0.1:8081`

**现象**: `-f` 文件模式或 interactive 模式下 `SELECT` 执行时报 `java.net.ConnectException`

**根因**: Flink `rest.bind-address: localhost` 只绑定 IPv6 `::1`，Java 优先用 IPv4 `127.0.0.1` 连接失败。

**排查**: 日志 `flink-ttt-sql-client-*.log` 中显示 `Connection refused: localhost/127.0.0.1:8081`

**解决**: 全部绑定改为 `0.0.0.0`，rest.address 用主机名：
```yaml
rest.bind-address: 0.0.0.0
rest.address: master
jobmanager.bind-host: 0.0.0.0
taskmanager.bind-host: 0.0.0.0
```

> 💡 CentOS 7 上 `localhost` 默认映射到 IPv6 `::1`，Java 客户端走 IPv4 就连不上。用 `0.0.0.0` 或显式 IP。

---

### 🟥 问题 9: Flink `TaskManager not registered` — slots = 0

**现象**: Web UI 显示 `slots-total: 0, taskmanagers: 0`，TaskExecutor 进程在但未注册到 JobManager。

**根因**: `flink-conf.yaml` 中有重复/冲突的 `jobmanager.rpc.address` 行（先 localhost 后 master），TaskManager 用第一个值连接失败。

**解决**:
```bash
grep -n "rpc.address\|bind-host" flink-conf.yaml | grep -v "^#"
# 删除重复行、统一地址后重启
```

> 💡 配置文件中的重复键，排在前面的生效。`sed -i` 追加容易产生重复。

---

### 🟥 问题 10: Flink Kafka `TimeoutException: Timed out waiting for a node assignment`

**现象**: 建表成功但查询报 `TimeoutException: Call: describeTopics`

**根因**: `bootstrap.servers = '127.0.0.1:9092'` 能建表，但 Kafka 内部 `advertised.listeners = master:9092`，Flink 连上 127.0.0.1 后 Kafka 返回 master 地址，跨主机 DNS 解析失败。

**解决**: `bootstrap.servers` 必须与 Kafka `advertised.listeners` 一致：
```sql
'properties.bootstrap.servers' = 'master:9092'
```

> 💡 Kafka broker 返回的 advertised 地址必须对 Flink 可达。先用 `nc -z master 9092` 确认。

---

### 🟥 问题 11: Flink SQL 文件 `-f` 模式 Shell 截断 SQL

**现象**: `-f /tmp/flink_job.sql` 执行后 SQL 不完整，`GROUP BY` 被截断。

**根因**: heredoc 写 SQL 文件时 shell 变量/特殊字符（如 `$`、反引号）可能被解释；python 写文件时 `'''` 三层引号嵌套错误。

**解决**: 用 `cat << 'SQLEOF' ... SQLEOF`（单引号保护 + 无冲突分隔符）：
```bash
cat > /tmp/flink_job.sql << 'SQLEOF'
SET 'sql-client.execution.result-mode' = 'tableau';
...
SQLEOF
# 验证完整性
grep "INTERVAL" /tmp/flink_job.sql
```

> 💡 写过 SQL 文件后必须 `grep` 关键行确认没有被 shell 截断。

---

## 💡 项目亮点

| 技术领域 | 实践内容 |
|----------|----------|
| 📊 **业务建模** | 5 表电商场景设计，维度表 + 事实表，存储过程批量造数 |
| 🔄 **批量采集** | DataX MySQL → HDFS，3 通道并行，JSON 配置化 |
| 📨 **实时通道** | Kafka 3 Broker 集群，2 Topic，Python 模拟实时生产者 |
| 🌊 **流处理** | Flink SQL 窗口聚合（已验证：每分钟 UV/PV 实时输出） |
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
> ⭐ **项目状态**: 数据采集 ✅ | 实时管道 ✅ | Flink窗口聚合 ✅ | 已完成
