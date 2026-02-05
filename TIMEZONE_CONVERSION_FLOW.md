# 时区转换说明 / Timezone Conversion Explanation

## 需求确认 (Requirement Confirmation)

**Oracle存储方式**: 以常规日期方式存储UTC时间
**飞书输入方式**: 程序将时间转换为+8时间（北京时间）后输入飞书
**重要**: 不更改Oracle中的记录，程序只读取Oracle数据

## 字段匹配说明 (Field Matching Explanation)

**重要**: 飞书本身不会自动匹配字段，所有字段匹配和类型转换都由本程序完成：
- 程序读取Oracle表结构
- 程序进行类型映射（Oracle类型 → 飞书类型）
- 程序调用飞书API创建字段
- 程序进行数据同步

## 当前实现流程 (Current Implementation Flow)

```
┌─────────────────────────────────────────────────────────────────┐
│                      数据同步流程                                 │
│                   Data Synchronization Flow                      │
└─────────────────────────────────────────────────────────────────┘

1. 从Oracle读取数据 (Read from Oracle - READ ONLY)
   ↓
   Oracle记录: CREATED_AT = 2024-01-01 00:00:00 (UTC时间)
   
2. 在内存中进行时区转换 (Convert in Memory)
   ↓
   转换后: CREATED_AT = 2024-01-01T08:00:00+08:00 (北京时间)
   
3. 写入飞书 (Write to Feishu)
   ↓
   飞书记录: CREATED_AT = 2024-01-01 08:00:00 (显示为北京时间)

4. Oracle原始数据保持不变 (Oracle Data Unchanged)
   ↓
   Oracle记录: CREATED_AT = 2024-01-01 00:00:00 (仍然是UTC时间)

✅ Oracle数据不会被修改
✅ 只进行只读查询（SELECT）
✅ 时间转换只在内存中进行
✅ 飞书中显示的是+8时间
```

## 代码实现细节 (Implementation Details)

### 1. Oracle读取（只读）

```python
# oracle_reader.py - read_batch() 方法
def read_batch(self, ...):
    # 执行SELECT查询
    self.cursor.execute(query, **params)
    rows = self.cursor.fetchall()
    
    for row in rows:
        for i, col in enumerate(columns):
            value = row[i]
            
            # 如果是日期时间对象
            if hasattr(value, 'isoformat'):
                # 在内存中转换时区（不影响Oracle）
                if self.convert_utc_to_utc8:
                    value = self.convert_utc_datetime_to_utc8(value)
                    
                # 转换为毫秒级时间戳（飞书API要求）
                value = int(value.timestamp() * 1000)
            
            record[col] = value
    
    return records  # 返回转换后的数据
```

**关键点**:
- ✅ 只使用 `SELECT` 查询读取数据
- ✅ 时区转换在内存中的 `value` 变量上进行
- ✅ 转换后的数据存储在 `records` 列表中
- ✅ **没有** UPDATE/INSERT 语句修改Oracle
- ✅ Oracle原始数据保持不变

### 2. 时区转换（内存操作）

```python
# oracle_reader.py - convert_utc_datetime_to_utc8() 方法
def convert_utc_datetime_to_utc8(self, dt: datetime) -> datetime:
    """将UTC时间转换为东八区时间（只在内存中进行）"""
    if dt is None:
        return None
    
    # 如果没有时区信息，假定为UTC
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    
    # 转换到东八区（在内存中创建新对象）
    dt_utc8 = dt.astimezone(TIMEZONE_UTC8)
    
    return dt_utc8  # 返回新的时间对象，不影响原数据
```

**关键点**:
- ✅ 创建新的 `datetime` 对象，不修改原对象
- ✅ 只在内存中进行时区计算
- ✅ 不涉及任何数据库写操作

### 3. 写入飞书（转换后的数据）

```python
# feishu_client.py - batch_create_records() 方法
def batch_create_records(self, table_id: str, records: List[Dict[str, Any]]):
    """批量创建飞书记录（使用已转换的数据）"""
    
    # records 中的时间已经是转换后的UTC+8时间
    sdk_records = []
    for record in records:
        app_table_record = AppTableRecord.builder() \
            .fields(record) \  # record包含转换后的时间
            .build()
        sdk_records.append(app_table_record)
    
    # 调用飞书API写入数据
    response = self.client.bitable.v1.app_table_record.batch_create(request)
    
    return record_ids
```

**关键点**:
- ✅ `records` 参数已包含转换后的UTC+8时间
- ✅ 直接写入飞书，不回写Oracle
- ✅ 飞书中存储和显示的是北京时间

## 数据示例 (Data Example)

### Oracle中的数据（UTC时间，不会被修改）

```sql
-- Oracle表中的记录
ID  | NAME  | CREATED_AT          | UPDATED_AT
----|-------|---------------------|--------------------
1   | Alice | 2024-01-01 00:00:00 | 2024-01-01 12:00:00
2   | Bob   | 2024-01-01 06:00:00 | 2024-01-01 18:00:00
```

### 飞书中的数据（UTC+8时间，以毫秒级时间戳存储）

```
ID  | NAME  | CREATED_AT      | UPDATED_AT
----|-------|-----------------|------------------
1   | Alice | 1704067200000   | 1704110400000
2   | Bob   | 1704088800000   | 1704153600000

注： 飞书API要求日期字段使用毫秒级时间戳格式
    1704067200000 对应 2024-01-01T08:00:00+08:00
    1704110400000 对应 2024-01-01T20:00:00+08:00
```

### 对比

| 字段 | Oracle (UTC) | 飞书 (UTC+8, 毫秒时间戳) | 时差 |
|------|--------------|--------------|------|
| CREATED_AT | 2024-01-01 00:00:00 | 1704067200000 (2024-01-01 08:00:00+08:00) | +8小时 |
| UPDATED_AT | 2024-01-01 12:00:00 | 1704110400000 (2024-01-01 20:00:00+08:00) | +8小时 |
| UPDATED_AT | 2024-01-01 18:00:00 | 1704153600000 (2024-01-02 02:00:00+08:00) | +8小时（跨天）|

## 验证方法 (Verification Method)

### 1. 查看Oracle数据（同步前）
```sql
SELECT ID, NAME, CREATED_AT FROM your_table WHERE ID = 1;
-- 结果: 2024-01-01 00:00:00
```

### 2. 执行数据同步
```bash
python sync_oracle_to_feishu.py
```

### 3. 查看Oracle数据（同步后）
```sql
SELECT ID, NAME, CREATED_AT FROM your_table WHERE ID = 1;
-- 结果: 2024-01-01 00:00:00 (没有变化！)
```

### 4. 查看飞书数据
```
飞书表中的记录:
ID: 1
NAME: Alice
CREATED_AT: 1704067200000 (飞书UI会显示为: 2024-01-01 08:00:00，北京时间)

注： 飞书API接收的是毫秒级时间戳，但在UI上会自动转换为可读的日期时间格式
```

## 总结 (Summary)

✅ **Oracle数据保持不变**: 原始UTC时间不会被修改
✅ **只读操作**: 所有Oracle操作都是SELECT查询
✅ **内存转换**: 时区转换只在内存中进行
✅ **飞书显示正确时间**: 飞书中显示的是UTC+8时间（北京时间）

**数据流向**:
```
Oracle (UTC, 只读)
    ↓ (SELECT查询)
内存 (进行时区转换: UTC → UTC+8)
    ↓ (写入操作)
飞书 (UTC+8, 北京时间)
```

**安全保障**:
- Oracle数据源安全：只有SELECT权限，不会修改数据
- 时区转换透明：用户无需手动调整时间
- 向后兼容：可以通过配置禁用时区转换

---

日期: 2024
说明: 本系统确保Oracle数据不会被修改，时区转换只在数据同步过程中进行
