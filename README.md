# Oracle to Feishu Data Sync (Oracle数据同步到飞书多维表格)

一个用于将Oracle数据库数据同步到飞书多维表格的Python程序，支持断点续传、增量同步、自动表管理等功能。

## 功能特性 (Features)

- ✅ **Oracle数据库读取**: 批量读取Oracle数据表，支持百万级数据
- ✅ **飞书API集成**: 使用官方飞书SDK (lark-oapi) 连接
- ✅ **速率限制**: 遵守飞书API限制（每秒50次请求，每批次500条记录）
- ✅ **自动表管理**: 当数据超过2万行时自动创建新表，表名包含序号
- ✅ **断点续传**: 支持中断后恢复，避免重复同步
- ✅ **增量同步**: 只同步新增或更新的数据
- ✅ **完整日志**: 详细的同步日志记录

## 系统要求 (Requirements)

- Python 3.7+
- Oracle Database (支持 Oracle 11g+)
- Oracle Instant Client (cx_Oracle依赖)
- 飞书开放平台应用凭证

## 安装 (Installation)

### 1. 安装Oracle Instant Client

根据你的操作系统下载并安装Oracle Instant Client:
https://www.oracle.com/database/technologies/instant-client/downloads.html

### 2. 安装Python依赖

```bash
pip install -r requirements.txt
```

## 配置 (Configuration)

### 1. 创建飞书应用

1. 访问 [飞书开放平台](https://open.feishu.cn/)
2. 创建企业自建应用
3. 获取 `App ID` 和 `App Secret`
4. 添加应用权限：
   - `bitable:app` (读取、创建多维表格)
   - `bitable:app:readonly` (读取多维表格信息)

### 2. 创建多维表格

1. 在飞书中创建一个多维表格（Bitable）
2. 获取多维表格的 `app_token` (在URL中)
3. 创建一个基础数据表，获取 `table_id`

### 3. 配置文件

复制 `config.yaml` 并修改配置:

```yaml
# Oracle数据库配置
oracle:
  host: "your-oracle-host"
  port: 1521
  service_name: "YOUR_SERVICE"
  username: "your_username"
  password: "your_password"
  table_name: "YOUR_TABLE"
  primary_key: "ID"          # 主键列
  sync_column: "UPDATED_AT"  # 用于增量同步的列（时间戳或递增ID）

# 飞书配置
feishu:
  app_id: "cli_xxxxx"
  app_secret: "xxxxx"
  app_token: "xxxxx"         # 多维表格app_token
  base_table_id: "xxxxx"     # 初始数据表ID
  table_name_prefix: "DataSync"
  max_rows_per_table: 20000

# 同步配置
sync:
  read_batch_size: 1000      # Oracle批量读取大小
  write_batch_size: 500      # 飞书批量写入大小
  checkpoint_file: "sync_checkpoint.json"
  max_requests_per_second: 50
```

## 使用方法 (Usage)

### 首次完整同步

```bash
python sync_oracle_to_feishu.py --full-sync
```

### 增量同步（推荐）

```bash
python sync_oracle_to_feishu.py
```

程序会自动从上次中断的位置继续同步。

### 指定配置文件

```bash
python sync_oracle_to_feishu.py --config my_config.yaml
```

### 重置检查点

```bash
python sync_oracle_to_feishu.py --reset-checkpoint
```

### 定时任务

使用 cron (Linux/Mac) 或任务计划程序 (Windows) 设置定时同步:

```bash
# 每小时执行一次增量同步
0 * * * * cd /path/to/project && python sync_oracle_to_feishu.py
```

## 工作原理 (How It Works)

### 1. 数据读取流程

```
Oracle Database
    ↓ (批量读取，按sync_column排序)
Batch Processing (1000条/批)
    ↓
Memory Buffer
```

### 2. 数据写入流程

```
Memory Buffer
    ↓ (分批500条)
Feishu API (Rate Limited: 50 req/s)
    ↓
Current Table (检查行数)
    ↓
如果超过20000行 → 创建新表 (表名_001, _002...)
```

### 3. 断点续传机制

```json
{
  "last_sync_value": "2024-01-01 12:00:00",
  "total_records_synced": 50000,
  "current_table_sequence": 3,
  "current_table_id": "tblxxxxxx",
  "last_batch_offset": 51000
}
```

## 项目结构 (Project Structure)

```
.
├── sync_oracle_to_feishu.py   # 主程序
├── oracle_reader.py            # Oracle数据读取模块
├── feishu_client.py            # 飞书API客户端（使用官方SDK）
├── checkpoint_manager.py       # 检查点管理
├── config.yaml                 # 配置文件
├── requirements.txt            # Python依赖
└── README.md                   # 文档
```

## 核心模块说明

### OracleDataReader

负责连接Oracle数据库并批量读取数据。

**主要方法:**
- `connect()`: 建立数据库连接
- `read_batch()`: 批量读取数据
- `get_total_count()`: 获取总记录数
- `get_max_value()`: 获取列最大值（用于检查点）

### FeishuClient

使用飞书官方SDK管理飞书多维表格。

**主要方法:**
- `batch_create_records()`: 批量创建记录（最多500条）
- `create_table()`: 创建新数据表
- `get_table_row_count()`: 获取表行数
- `write_records_with_table_management()`: 自动管理表切换

### CheckpointManager

管理同步检查点，实现断点续传。

**主要方法:**
- `update_sync_progress()`: 更新同步进度
- `get_last_sync_value()`: 获取上次同步位置
- `reset()`: 重置检查点

## 注意事项 (Notes)

1. **数据类型映射**: 程序会自动推断Oracle字段类型并映射到飞书字段类型
2. **大对象处理**: LOB类型会被读取为文本
3. **时间格式**: 时间字段会转换为ISO 8601格式
4. **表命名规则**: 新表命名格式为 `{prefix}_{序号:03d}` (如: DataSync_001)
5. **速率限制**: 内置速率限制器确保不超过飞书API限制
6. **错误重试**: 建议在外部（如cron）实现失败重试机制

## 故障排查 (Troubleshooting)

### Oracle连接失败

```
检查：
1. Oracle Instant Client是否正确安装
2. tnsnames.ora配置是否正确
3. 网络连接和防火墙设置
```

### 飞书API调用失败

```
检查：
1. App ID和App Secret是否正确
2. 应用权限是否足够
3. app_token和table_id是否有效
```

### 同步速度慢

```
优化：
1. 调整read_batch_size和write_batch_size
2. 增加并发（需修改代码）
3. 检查网络延迟
```

## 许可证 (License)

MIT License

## 参考文档 (References)

- [飞书开放平台文档](https://open.feishu.cn/document/home/index)
- [飞书多维表格API](https://open.feishu.cn/document/server-docs/docs/bitable-v1/bitable-overview)
- [Oracle cx_Oracle文档](https://cx-oracle.readthedocs.io/)
- [Feishu SDK (lark-oapi)](https://github.com/larksuite/oapi-sdk-python)
