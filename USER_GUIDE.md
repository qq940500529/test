# 使用说明 (User Guide)

## 快速开始 (Quick Start)

### 1. 环境准备 (Environment Setup)

#### 安装Python依赖
```bash
pip install -r requirements.txt
```

**注意**: 本项目使用 `oracledb` (Oracle官方新驱动)，采用Thin模式，无需安装Oracle Instant Client。

### 2. 配置 (Configuration)

#### 复制配置文件模板
```bash
cp config.yaml.example config.yaml
```

#### 编辑配置文件 (Edit config.yaml)

```yaml
oracle:
  host: "your-oracle-host"      # Oracle主机地址
  port: 1521                     # 端口
  service_name: "YOUR_SERVICE"   # 服务名
  username: "your_username"      # 用户名
  password: "your_password"      # 密码
  table_name: "YOUR_TABLE"       # 表名
  primary_key: "ID"              # 主键列
  sync_column: "UPDATED_AT"      # 增量同步列
  convert_utc_to_utc8: true      # UTC到+8时区转换（默认启用）

feishu:
  app_id: "cli_xxxxx"            # 飞书应用ID
  app_secret: "xxxxx"            # 飞书应用密钥
  app_token: "xxxxx"             # 多维表格token
  base_table_id: "xxxxx"         # 初始数据表ID（可选 - 不提供将自动创建）
  table_name_prefix: "DataSync"  # 表名前缀
  max_rows_per_table: 20000      # 每表最大行数
```

### 3. 运行同步 (Run Sync)

#### 首次完整同步 (First Full Sync)
```bash
python sync_oracle_to_feishu.py --full-sync
```

#### 增量同步 (Incremental Sync)
```bash
python sync_oracle_to_feishu.py
```

#### 使用自定义配置 (Custom Config)
```bash
python sync_oracle_to_feishu.py --config my_config.yaml
```

#### 重置检查点 (Reset Checkpoint)
```bash
python sync_oracle_to_feishu.py --reset-checkpoint
```

### 4. 查看日志 (View Logs)

```bash
tail -f sync.log
```

## 核心功能说明 (Core Features)

### 0. 自动字段匹配与表创建 (Auto Field Matching and Table Creation)

**新特性**: 现在可以不提供 `base_table_id`，系统会根据Oracle表结构自动创建飞书表。

- **自动字段匹配**: 读取Oracle表的字段架构（字段名和类型），在飞书中创建对应的字段
- **字段类型映射**: 自动将Oracle字段类型映射到飞书字段类型
  - Oracle NUMBER/INTEGER → 飞书数字（Number）
  - Oracle DATE/TIMESTAMP → 飞书日期（Date）
  - Oracle VARCHAR2/CHAR/CLOB → 飞书文本（Text）
- **一一对应**: 飞书表的字段与Oracle表的字段完全对应，字段名保持一致
- **自动创建表**: 如果配置中没有 `base_table_id`，系统会自动创建表并匹配所有字段
- **向后兼容**: 仍然支持提供 `base_table_id` 的传统方式

**配置方式**:
```yaml
feishu:
  app_id: "cli_xxxxx"
  app_secret: "xxxxx"
  app_token: "xxxxx"
  # base_table_id: "xxxxx"  # 注释掉或删除此行以启用自动创建
  table_name_prefix: "DataSync"
  max_rows_per_table: 20000
```

**工作原理**:
1. 连接Oracle数据库，读取表的完整字段架构（包括字段名和数据类型）
2. 根据Oracle字段类型自动映射到飞书字段类型
3. 自动创建表 `DataSync_001` 并添加所有对应的字段
4. 开始数据同步，字段一一对应

**示例**:
如果Oracle表有以下字段：
- ID (NUMBER) → 飞书数字字段 ID
- NAME (VARCHAR2) → 飞书文本字段 NAME
- CREATED_AT (TIMESTAMP) → 飞书日期字段 CREATED_AT

飞书表将自动创建完全对应的字段。

### 1. 时区转换 (Timezone Conversion)

**重要特性**: Oracle中存储的UTC时间会自动转换为东八区时间（北京时间，UTC+8）。

- **自动转换**: 所有日期/时间字段会自动进行时区转换
- **配置选项**: 可通过配置文件控制是否启用时区转换
- **时间格式**: 转换后的时间采用ISO 8601格式，包含时区信息

**配置方式**:
```yaml
oracle:
  # ... 其他配置 ...
  convert_utc_to_utc8: true  # 启用UTC到+8时区转换（默认）
  # convert_utc_to_utc8: false  # 禁用时区转换，保持原始UTC时间
```

**转换示例**:
- Oracle UTC时间: `2024-01-01 00:00:00` (UTC)
- 转换后时间: `2024-01-01T08:00:00+08:00` (北京时间)

**注意事项**:
1. 只有日期/时间类型字段会被转换（DATE, TIMESTAMP等）
2. 文本、数字等其他类型字段不受影响
3. 如果Oracle中的时间已经是本地时间，请设置 `convert_utc_to_utc8: false`

### 2. 断点续传 (Resumable Transfer)

程序使用检查点文件 `sync_checkpoint.json` 记录同步进度：
- 最后同步的数据值
- 当前表ID和序号
- 总同步记录数
- 同步历史

如果同步中断，下次运行时会自动从上次中断的位置继续。

### 3. 增量同步 (Incremental Sync)

通过配置 `sync_column`（如时间戳或递增ID），程序只同步新增或更新的数据：

```sql
-- 增量同步查询示例
SELECT * FROM table WHERE UPDATED_AT > '上次同步值'
```

### 4. 自动表管理 (Automatic Table Management)

当数据超过2万行（飞书限制）时，自动创建新表：
- DataSync_001 (前2万行)
- DataSync_002 (2万-4万行)
- DataSync_003 (4万-6万行)
- ...

### 5. 速率限制 (Rate Limiting)

遵守飞书API限制：
- 每秒最多50次请求
- 每批次最多500条记录

### 6. 数据类型映射 (Data Type Mapping)

| Oracle类型 | 飞书类型 |
|-----------|---------|
| NUMBER, INTEGER | 数字 (Number) |
| VARCHAR2, CHAR, CLOB | 文本 (Text) |
| DATE, TIMESTAMP | 日期 (Date) |
| BOOLEAN | 复选框 (Checkbox) |

## 定时任务设置 (Scheduled Tasks)

### Linux/Mac (Cron)

```bash
# 编辑crontab
crontab -e

# 每小时执行一次增量同步
0 * * * * cd /path/to/project && /usr/bin/python3 sync_oracle_to_feishu.py >> /var/log/oracle_feishu_sync.log 2>&1

# 每天凌晨2点执行
0 2 * * * cd /path/to/project && /usr/bin/python3 sync_oracle_to_feishu.py
```

### Windows (Task Scheduler)

1. 打开"任务计划程序"
2. 创建基本任务
3. 设置触发器（如每小时）
4. 操作：启动程序
   - 程序：`python.exe`
   - 参数：`sync_oracle_to_feishu.py`
   - 起始于：项目目录路径

## 监控和维护 (Monitoring & Maintenance)

### 查看同步状态

```python
from checkpoint_manager import CheckpointManager

manager = CheckpointManager('sync_checkpoint.json')
data = manager.get_checkpoint_data()

print(f"总同步记录: {data['total_records_synced']}")
print(f"最后同步时间: {data['last_sync_time']}")
print(f"当前表序号: {data['current_table_sequence']}")
```

### 日志分析

```bash
# 查看错误
grep "ERROR" sync.log

# 查看同步统计
grep "completed successfully" sync.log

# 查看最近的同步
tail -n 100 sync.log
```

### 性能优化

1. **调整批次大小**
```yaml
sync:
  read_batch_size: 2000  # 增大读取批次
  write_batch_size: 500  # 保持写入批次为500（API限制）
```

2. **优化数据库查询**
- 在 `sync_column` 上创建索引
- 使用合适的 `primary_key`

```sql
CREATE INDEX idx_updated_at ON your_table(UPDATED_AT);
```

## 故障排查 (Troubleshooting)

### 问题1: Oracle连接失败

**错误**: `ORA-12154: TNS:could not resolve the connect identifier` 或连接超时

**解决方案**:
1. 验证Oracle数据库地址、端口和服务名是否正确
2. 检查网络连接和防火墙设置
3. 如果使用Thin模式（默认），确保可以直接访问数据库
4. 测试连接：
```python
import oracledb
conn = oracledb.connect(user="username", password="password", 
                        dsn="host:port/service_name")
print("Connection successful!")
conn.close()
```

**可选 - 使用Thick模式**:
如果需要使用Oracle Instant Client的高级功能：
```python
import oracledb
oracledb.init_oracle_client(lib_dir="/path/to/instantclient")
```

### 问题2: 飞书API认证失败

**错误**: `Failed to get access token`

**解决方案**:
1. 验证 `app_id` 和 `app_secret`
2. 检查应用权限：必须有 `bitable:app` 权限
3. 确认应用已启用

### 问题3: 同步速度慢

**原因分析**:
- 网络延迟
- 批次大小设置不当
- 数据库查询无索引

**优化方案**:
1. 增加 `read_batch_size`
2. 在同步列上创建索引
3. 检查网络连接质量

### 问题4: 检查点文件损坏

**解决方案**:
```bash
# 备份现有检查点
cp sync_checkpoint.json sync_checkpoint.json.bak

# 重置检查点
python sync_oracle_to_feishu.py --reset-checkpoint

# 重新开始同步
python sync_oracle_to_feishu.py
```

## 最佳实践 (Best Practices)

1. **首次同步使用完整模式**
```bash
python sync_oracle_to_feishu.py --full-sync
```

2. **定期备份检查点文件**
```bash
cp sync_checkpoint.json backups/checkpoint_$(date +%Y%m%d).json
```

3. **监控日志文件大小**
```bash
# 定期清理日志
> sync.log
```

4. **测试环境先验证**
- 先在测试环境配置
- 验证数据类型映射
- 检查同步速度和准确性

5. **设置告警机制**
```bash
# 监控同步失败
if ! python sync_oracle_to_feishu.py; then
    echo "Sync failed!" | mail -s "Oracle-Feishu Sync Alert" admin@example.com
fi
```

## 高级配置 (Advanced Configuration)

### 自定义字段映射

如需自定义字段类型映射，修改 `feishu_client.py` 中的 `_infer_field_type` 方法：

```python
def _infer_field_type(self, value: Any) -> int:
    # 自定义类型推断逻辑
    if isinstance(value, str) and '@' in value:
        return 15  # URL类型
    # ... 其他自定义逻辑
```

### 多表同步

创建多个配置文件，分别同步不同的表：

```bash
# 同步表1
python sync_oracle_to_feishu.py --config config_table1.yaml

# 同步表2
python sync_oracle_to_feishu.py --config config_table2.yaml
```

### 并行同步

使用 `parallel` 或 `xargs` 并行处理：

```bash
# GNU Parallel
parallel python sync_oracle_to_feishu.py --config {} ::: config1.yaml config2.yaml config3.yaml
```

## 技术支持 (Technical Support)

如遇到问题，请提供以下信息：
1. 错误日志 (`sync.log`)
2. 配置文件（隐藏敏感信息）
3. Python版本和依赖版本
4. Oracle和飞书环境信息

## 参考资源 (References)

- [飞书开放平台](https://open.feishu.cn/)
- [飞书多维表格API文档](https://open.feishu.cn/document/server-docs/docs/bitable-v1/bitable-overview)
- [Oracle python-oracledb文档](https://python-oracledb.readthedocs.io/)
- [Oracle cx_Oracle文档](https://cx-oracle.readthedocs.io/) (旧版本参考)
- [Feishu Python SDK](https://github.com/larksuite/oapi-sdk-python)
