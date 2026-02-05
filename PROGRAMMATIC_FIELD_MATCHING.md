# 程序化字段匹配说明 / Programmatic Field Matching Explanation

## 重要说明 (Important Notice)

**飞书不会自动匹配字段**

本系统中的所有字段匹配、类型转换和表创建都是由**程序**完成的，不依赖飞书的自动匹配功能。

## 程序工作流程 (Program Workflow)

```
┌─────────────────────────────────────────────────────────────┐
│  第一步：程序读取Oracle表结构                                  │
│  Step 1: Program Reads Oracle Table Structure                │
└─────────────────────────────────────────────────────────────┘

程序连接Oracle数据库
    ↓
程序执行SQL查询 user_tab_columns
    ↓
程序获取字段信息：
  - 字段名称 (column_name): ID, NAME, EMAIL, CREATED_AT
  - 数据类型 (data_type): NUMBER, VARCHAR2, VARCHAR2, TIMESTAMP
  - 数据长度/精度 (data_length/precision)

┌─────────────────────────────────────────────────────────────┐
│  第二步：程序进行类型映射                                      │
│  Step 2: Program Maps Types                                  │
└─────────────────────────────────────────────────────────────┘

程序内置映射规则（在 feishu_client.py 中）：

def map_oracle_type_to_feishu(oracle_type):
    if oracle_type in ('NUMBER', 'INTEGER', 'FLOAT'):
        return FIELD_TYPE_NUMBER  # 2 - 数字
    elif oracle_type in ('DATE', 'TIMESTAMP'):
        return FIELD_TYPE_DATE    # 5 - 日期
    elif oracle_type in ('VARCHAR2', 'CHAR', 'CLOB'):
        return FIELD_TYPE_TEXT    # 1 - 文本
    # ...

程序执行映射：
  ID (NUMBER)       → 飞书类型 2 (数字)
  NAME (VARCHAR2)   → 飞书类型 1 (文本)
  EMAIL (VARCHAR2)  → 飞书类型 1 (文本)
  CREATED_AT (TIMESTAMP) → 飞书类型 5 (日期)

┌─────────────────────────────────────────────────────────────┐
│  第三步：程序调用飞书API创建表                                 │
│  Step 3: Program Calls Feishu API to Create Table            │
└─────────────────────────────────────────────────────────────┘

程序构建飞书API请求：
{
  "table": {
    "name": "DataSync_001",
    "fields": [
      {"field_name": "ID", "type": 2},
      {"field_name": "NAME", "type": 1},
      {"field_name": "EMAIL", "type": 1},
      {"field_name": "CREATED_AT", "type": 5}
    ]
  }
}

程序发送HTTP POST请求到飞书API
    ↓
飞书服务器创建表和字段
    ↓
飞书返回表ID给程序

┌─────────────────────────────────────────────────────────────┐
│  第四步：程序同步数据                                          │
│  Step 4: Program Synchronizes Data                           │
└─────────────────────────────────────────────────────────────┘

程序从Oracle读取数据
    ↓
程序进行时区转换（UTC → UTC+8）
    ↓
程序调用飞书API写入数据
```

## 关键代码实现 (Key Code Implementation)

### 1. 程序读取Oracle表结构

**文件**: `oracle_reader.py`

```python
def get_table_schema(self, table_name: str) -> List[Dict[str, Any]]:
    """程序查询Oracle数据字典获取表结构"""
    query = """
        SELECT column_name, data_type, data_length, data_precision, data_scale
        FROM user_tab_columns 
        WHERE table_name = UPPER(:table_name)
        ORDER BY column_id
    """
    self.cursor.execute(query, table_name=table_name)
    
    columns_info = []
    for row in self.cursor.fetchall():
        column_info = {
            'column_name': row[0],   # 程序提取字段名
            'data_type': row[1],     # 程序提取数据类型
            'data_length': row[2],   # 程序提取长度
            'data_precision': row[3],# 程序提取精度
            'data_scale': row[4]     # 程序提取小数位
        }
        columns_info.append(column_info)
    
    return columns_info
```

### 2. 程序进行类型映射

**文件**: `feishu_client.py`

```python
def map_oracle_type_to_feishu(self, oracle_type: str) -> int:
    """程序内置的类型映射规则"""
    oracle_type_upper = oracle_type.upper()
    
    # 程序判断：数字类型
    if oracle_type_upper in ('NUMBER', 'INTEGER', 'FLOAT', 'DECIMAL'):
        return FIELD_TYPE_NUMBER  # 程序返回飞书数字类型代码 2
    
    # 程序判断：日期时间类型
    elif oracle_type_upper in ('DATE', 'TIMESTAMP'):
        return FIELD_TYPE_DATE    # 程序返回飞书日期类型代码 5
    
    # 程序判断：文本类型
    elif oracle_type_upper in ('VARCHAR2', 'CHAR', 'CLOB'):
        return FIELD_TYPE_TEXT    # 程序返回飞书文本类型代码 1
    
    # 程序默认：未知类型映射为文本
    else:
        logger.warning(f"程序遇到未知类型 '{oracle_type}'，映射为文本类型")
        return FIELD_TYPE_TEXT
```

### 3. 程序创建飞书表

**文件**: `feishu_client.py`

```python
def create_table_from_oracle_schema(
    self, 
    oracle_columns: List[Dict[str, Any]], 
    sequence_number: int = 1
) -> str:
    """程序根据Oracle架构创建飞书表"""
    table_name = f"{self.table_name_prefix}_{sequence_number:03d}"
    
    # 程序构建字段列表
    fields = []
    for col in oracle_columns:
        field_name = col['column_name']
        oracle_type = col['data_type']
        # 程序调用映射方法
        feishu_type = self.map_oracle_type_to_feishu(oracle_type)
        
        fields.append({
            "field_name": field_name,
            "type": feishu_type
        })
        
        logger.info(f"程序映射字段: {field_name} ({oracle_type} → 飞书类型 {feishu_type})")
    
    # 程序调用飞书SDK创建表
    table_id = self.create_table(table_name, fields)
    
    logger.info(f"程序创建飞书表成功: {table_name} (ID: {table_id})")
    return table_id
```

### 4. 程序调用飞书API

**文件**: `feishu_client.py`

```python
def create_table(self, table_name: str, fields: List[Dict[str, Any]]) -> str:
    """程序通过飞书SDK创建表"""
    
    # 程序将字段转换为SDK格式
    sdk_fields = []
    for field in fields:
        app_table_create_header = AppTableCreateHeader.builder() \
            .field_name(field['field_name']) \
            .type(field['type']) \
            .build()
        sdk_fields.append(app_table_create_header)
    
    # 程序构建创建表的请求
    request = CreateAppTableRequest.builder() \
        .app_token(self.app_token) \
        .request_body(ReqAppTable.builder()
            .table(ReqAppTableTable.builder()
                .name(table_name)
                .default_view_name("Default View")
                .fields(sdk_fields)  # 程序提供的字段定义
                .build())
            .build()) \
        .build()
    
    # 程序发送HTTP请求到飞书API
    response = self.client.bitable.v1.app_table.create(request)
    
    if not response.success():
        raise Exception(f"程序创建表失败: {response.msg}")
    
    # 程序获取飞书返回的表ID
    table_id = response.data.table_id
    return table_id
```

## 完整流程示例 (Complete Workflow Example)

### Oracle表结构

```sql
-- Oracle中的表定义
CREATE TABLE employees (
    ID NUMBER(10) PRIMARY KEY,
    NAME VARCHAR2(100) NOT NULL,
    EMAIL VARCHAR2(255),
    SALARY NUMBER(10, 2),
    HIRE_DATE DATE,
    UPDATED_AT TIMESTAMP
);
```

### 程序执行步骤

1. **程序读取表结构**
```python
oracle_schema = oracle_reader.get_table_schema('employees')
# 结果：
# [
#   {'column_name': 'ID', 'data_type': 'NUMBER', ...},
#   {'column_name': 'NAME', 'data_type': 'VARCHAR2', ...},
#   {'column_name': 'EMAIL', 'data_type': 'VARCHAR2', ...},
#   {'column_name': 'SALARY', 'data_type': 'NUMBER', ...},
#   {'column_name': 'HIRE_DATE', 'data_type': 'DATE', ...},
#   {'column_name': 'UPDATED_AT', 'data_type': 'TIMESTAMP', ...}
# ]
```

2. **程序进行类型映射**
```python
for col in oracle_schema:
    feishu_type = feishu_client.map_oracle_type_to_feishu(col['data_type'])
    # ID (NUMBER) → 2 (数字)
    # NAME (VARCHAR2) → 1 (文本)
    # EMAIL (VARCHAR2) → 1 (文本)
    # SALARY (NUMBER) → 2 (数字)
    # HIRE_DATE (DATE) → 5 (日期)
    # UPDATED_AT (TIMESTAMP) → 5 (日期)
```

3. **程序创建飞书表**
```python
table_id = feishu_client.create_table_from_oracle_schema(oracle_schema, 1)
# 程序调用飞书API创建表 "DataSync_001"
# 包含字段：ID(数字), NAME(文本), EMAIL(文本), SALARY(数字), HIRE_DATE(日期), UPDATED_AT(日期)
```

4. **飞书表结果**

飞书中创建的表 "DataSync_001":
- ID - 数字类型
- NAME - 文本类型
- EMAIL - 文本类型
- SALARY - 数字类型
- HIRE_DATE - 日期类型
- UPDATED_AT - 日期类型

## 总结 (Summary)

### 程序的职责 (Program's Responsibilities)

✅ **程序负责**:
- 连接Oracle数据库
- 查询表结构
- 进行类型映射
- 调用飞书API创建表和字段
- 读取和转换数据
- 写入数据到飞书

❌ **飞书不负责**:
- 飞书不会自动读取Oracle结构
- 飞书不会自动匹配字段
- 飞书不会自动转换类型
- 飞书只是提供API接口供程序调用

### 关键点 (Key Points)

1. **程序驱动**: 所有操作都由程序主动发起和控制
2. **显式映射**: 类型映射规则在程序代码中明确定义
3. **API调用**: 程序通过飞书API创建表和字段
4. **数据转换**: 程序在内存中进行时区转换和数据处理
5. **一对一对应**: 程序确保Oracle字段和飞书字段一一对应

---

日期: 2024
说明: 本文档明确说明飞书不会自动匹配字段，所有字段匹配都由程序完成
