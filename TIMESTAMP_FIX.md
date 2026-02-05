# 增量同步时间戳类型修复 / Incremental Sync Timestamp Type Fix

## 问题描述 / Problem Description

在使用增量同步时，出现以下错误：

```
ORA-00932: 数据类型不一致: 应为 DATE, 但却获得 NUMBER
```

### 根本原因 / Root Cause

当 `sync_column` 是 DATE 或 TIMESTAMP 类型时：
1. Oracle 数据库中的日期时间值被转换为毫秒时间戳（数字）存储在检查点文件中
2. 下次增量同步时，这个数字时间戳被直接用于 SQL WHERE 子句
3. Oracle 期望 DATE 类型但收到 NUMBER 类型，导致类型不匹配错误

## 解决方案 / Solution

在 `oracle_reader.py` 中实现了自动类型转换：

1. **表结构缓存**：自动缓存表的列类型信息
2. **智能类型检测**：检测 sync_column 是否为 DATE/TIMESTAMP 类型
3. **自动转换**：将毫秒时间戳自动转换为 datetime 对象用于查询

### 关键改动 / Key Changes

- 新增 `_table_schemas` 缓存表结构
- 新增 `_get_column_type()` 获取列类型
- 新增 `_convert_timestamp_to_date()` 转换时间戳
- 新增 `_prepare_sync_value_for_query()` 准备查询值
- 更新 `get_total_count()` 和 `read_batch()` 使用类型转换

## 使用说明 / Usage

无需任何配置更改，修复会自动生效。增量同步现在可以正常工作：

```bash
# 增量同步
python sync_oracle_to_feishu.py

# 完整同步
python sync_oracle_to_feishu.py --full-sync
```

## 技术细节 / Technical Details

### 支持的类型 / Supported Types
- `DATE`
- `TIMESTAMP` (所有精度变体 0-9)

### 转换逻辑 / Conversion Logic
```python
# 如果 sync_column 是 DATE/TIMESTAMP 类型
# 且 last_sync_value 是数字（毫秒时间戳）
# 则转换为 datetime 对象

if is_date_type and isinstance(last_sync_value, (int, float)):
    datetime_value = datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)
```

## 配置说明 / Configuration Notes

您可以在 `config.yaml` 中调整批次大小以优化性能：

```yaml
sync:
  read_batch_size: 1000   # 从 Oracle 读取的批次大小
  write_batch_size: 1000  # 写入飞书的批次大小（最大 1000）
```

**建议**：
- `read_batch_size`: 可以根据数据大小设置为 1000-10000
- `write_batch_size`: 建议保持为 1000（飞书 API 限制）

## 向后兼容性 / Backward Compatibility

✅ 完全向后兼容
- 不影响现有的 NUMBER 类型 sync_column
- 不影响已有的 VARCHAR2 等其他类型
- 现有的检查点文件可以继续使用

## 安全性 / Security

✅ 通过 CodeQL 安全扫描，无漏洞
