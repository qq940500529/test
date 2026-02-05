"""
Feishu (Lark) API Client
飞书API客户端

Handles interaction with Feishu multi-dimensional tables (Bitable)
处理与飞书多维表格的交互
Uses official Feishu SDK (lark-oapi)
使用官方飞书SDK (lark-oapi)
"""
import time
import logging
from typing import List, Dict, Any, Optional
from ratelimit import limits, sleep_and_retry
import lark_oapi as lark
from lark_oapi.api.bitable.v1 import *

logger = logging.getLogger(__name__)

# 飞书字段类型常量 / Feishu field type constants
FIELD_TYPE_TEXT = 1  # 文本
FIELD_TYPE_NUMBER = 2  # 数字
FIELD_TYPE_SINGLE_SELECT = 3  # 单选
FIELD_TYPE_MULTI_SELECT = 4  # 多选
FIELD_TYPE_DATE = 5  # 日期
FIELD_TYPE_CHECKBOX = 7  # 复选框
FIELD_TYPE_PERSON = 11  # 人员
FIELD_TYPE_PHONE = 13  # 电话
FIELD_TYPE_URL = 15  # URL
FIELD_TYPE_ATTACHMENT = 17  # 附件
FIELD_TYPE_SINGLE_LINK = 18  # 单向关联
FIELD_TYPE_FORMULA = 20  # 公式
FIELD_TYPE_TWO_WAY_LINK = 21  # 双向关联
FIELD_TYPE_LOCATION = 22  # 地理位置
FIELD_TYPE_GROUP_CHAT = 23  # 群组
FIELD_TYPE_CREATED_TIME = 1001  # 创建时间
FIELD_TYPE_MODIFIED_TIME = 1002  # 修改时间
FIELD_TYPE_CREATED_BY = 1003  # 创建人
FIELD_TYPE_MODIFIED_BY = 1004  # 修改人
FIELD_TYPE_AUTO_NUMBER = 1005  # 自动编号


class FeishuClient:
    """
    Feishu API client with rate limiting support using official SDK
    飞书API客户端，使用官方SDK并支持速率限制
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize Feishu client with SDK
        使用SDK初始化飞书客户端
        
        Args:
            config: Feishu configuration dictionary / 飞书配置字典
        """
        self.app_id = config['app_id']  # 应用ID
        self.app_secret = config['app_secret']  # 应用密钥
        self.app_token = config['app_token']  # 多维表格app_token
        self.base_table_id = config.get('base_table_id')  # 基础数据表ID（可选）
        self.table_name_prefix = config.get('table_name_prefix', 'DataSync')  # 表名前缀
        self.max_rows_per_table = config.get('max_rows_per_table', 20000)  # 每表最大行数
        self.max_requests_per_second = config.get('max_requests_per_second', 50)  # 每秒最大请求数
        
        # 初始化飞书SDK客户端
        self.client = lark.Client.builder() \
            .app_id(self.app_id) \
            .app_secret(self.app_secret) \
            .log_level(lark.LogLevel.INFO) \
            .build()
        
        # 跟踪当前表信息
        self.current_table_id = self.base_table_id  # 当前使用的表ID（可以为None）
        self.current_table_row_count = 0  # 当前表的行数
        
        # Rate limiting tracking / 速率限制追踪
        self._last_request_time = 0
        self._request_count = 0
    
    def _apply_rate_limit(self):
        """
        Apply rate limiting before API calls
        在API调用前应用速率限制
        """
        current_time = time.time()
        
        # Reset counter if more than 1 second has passed
        # 如果超过1秒，重置计数器
        if current_time - self._last_request_time >= 1.0:
            self._request_count = 0
            self._last_request_time = current_time
        
        # If reached limit, wait until next second
        # 如果达到限制，等待到下一秒
        if self._request_count >= self.max_requests_per_second:
            sleep_time = 1.0 - (current_time - self._last_request_time)
            if sleep_time > 0:
                time.sleep(sleep_time)
            self._request_count = 0
            self._last_request_time = time.time()
        
        self._request_count += 1
    
    def get_table_info(self, table_id: str) -> Dict[str, Any]:
        """
        Get table information using SDK
        使用SDK获取表信息
        
        Args:
            table_id: Table ID / 表ID
            
        Returns:
            Table information / 表信息
        """
        self._apply_rate_limit()  # 应用速率限制
        
        # 构建获取表信息的请求
        request = GetAppTableRequest.builder() \
            .app_token(self.app_token) \
            .table_id(table_id) \
            .build()
        
        # 调用SDK接口
        response = self.client.bitable.v1.app_table.get(request)
        
        # 检查响应是否成功
        if not response.success():
            logger.error(f"获取表信息失败 / Failed to get table info: {response.code}, {response.msg}")
            raise Exception(f"Failed to get table info: {response.msg}")
        
        return {
            "table_id": response.data.table.table_id,
            "name": response.data.table.name,
            "revision": response.data.table.revision
        }
    
    def get_table_row_count(self, table_id: str) -> int:
        """
        Get current row count in a table using SDK
        使用SDK获取表的当前行数
        
        Args:
            table_id: Table ID / 表ID
            
        Returns:
            Number of rows / 行数
        """
        self._apply_rate_limit()  # 应用速率限制
        
        # 构建列表记录请求，只获取1条用于获取总数
        request = ListAppTableRecordRequest.builder() \
            .app_token(self.app_token) \
            .table_id(table_id) \
            .page_size(1) \
            .build()
        
        response = self.client.bitable.v1.app_table_record.list(request)
        
        if not response.success():
            logger.error(f"获取行数失败 / Failed to get row count: {response.code}, {response.msg}")
            raise Exception(f"Failed to get row count: {response.msg}")
        
        return response.data.total if response.data.total else 0
    
    def create_table(self, table_name: str, fields: List[Dict[str, Any]]) -> str:
        """
        Create a new table in the bitable using SDK
        使用SDK在多维表格中创建新表
        
        Args:
            table_name: Name for the new table / 新表名称
            fields: List of field definitions / 字段定义列表
            
        Returns:
            New table ID / 新表ID
        """
        self._apply_rate_limit()  # 应用速率限制
        
        # 将字段转换为SDK格式
        sdk_fields = []
        for field in fields:
            app_table_create_header = AppTableCreateHeader.builder() \
                .field_name(field['field_name']) \
                .type(field['type']) \
                .build()
            sdk_fields.append(app_table_create_header)
        
        # 构建创建表的请求
        request = CreateAppTableRequest.builder() \
            .app_token(self.app_token) \
            .request_body(CreateAppTableRequestBody.builder()
                .table(ReqTable.builder()
                    .name(table_name)
                    .default_view_name("Default View")
                    .fields(sdk_fields)
                    .build())
                .build()) \
            .build()
        
        response = self.client.bitable.v1.app_table.create(request)
        
        if not response.success():
            logger.error(f"创建表失败 / Failed to create table: {response.code}, {response.msg}")
            raise Exception(f"Failed to create table: {response.msg}")
        
        table_id = response.data.table_id
        logger.info(f"创建新表 / Created new table: {table_name} (ID: {table_id})")
        return table_id
    
    def ensure_table_exists(self, sample_record: Dict[str, Any], sequence_number: int = 1) -> str:
        """
        Ensure a table exists, creating it if needed (fallback when oracle_schema is not available)
        确保表存在，如需要则创建（当oracle_schema不可用时的回退方法）
        
        Note:
            This is a fallback method that infers field types from sample data values.
            Prefer using create_table_from_oracle_schema() when Oracle schema is available.
            这是从样本数据值推断字段类型的回退方法。
            当Oracle架构可用时，优先使用create_table_from_oracle_schema()。
        
        Args:
            sample_record: Sample record to infer field types / 用于推断字段类型的样本记录
            sequence_number: Sequence number for table name / 表名序号
            
        Returns:
            Table ID / 表ID
        """
        if self.current_table_id is None:
            # No table exists yet, create one
            # 还没有表，创建一个
            table_name = f"{self.table_name_prefix}_{sequence_number:03d}"
            
            # Create fields from sample record
            # 根据样本记录创建字段
            fields = []
            for field_name, value in sample_record.items():
                field_type = self._infer_field_type(value)
                fields.append({
                    "field_name": field_name,
                    "type": field_type
                })
            
            # Create the table
            # 创建表
            self.current_table_id = self.create_table(table_name, fields)
            self.current_table_row_count = 0
            logger.info(f"自动创建初始表 / Auto-created initial table: {table_name} (ID: {self.current_table_id})")
        
        return self.current_table_id
    
    def create_table_from_oracle_schema(self, oracle_columns: List[Dict[str, Any]], sequence_number: int = 1) -> str:
        """
        Create a Feishu table based on Oracle table schema
        根据Oracle表架构创建飞书表
        
        Args:
            oracle_columns: List of Oracle column definitions with column_name and data_type
                           Oracle列定义列表，包含column_name和data_type
            sequence_number: Sequence number for table name / 表名序号
            
        Returns:
            Created table ID / 创建的表ID
        """
        table_name = f"{self.table_name_prefix}_{sequence_number:03d}"
        
        # Map Oracle columns to Feishu fields
        # 将Oracle列映射到飞书字段
        fields = []
        for col in oracle_columns:
            field_name = col['column_name']
            oracle_type = col['data_type']
            feishu_type = self.map_oracle_type_to_feishu(oracle_type)
            
            fields.append({
                "field_name": field_name,
                "type": feishu_type
            })
            
            logger.info(f"映射字段 / Mapped field: {field_name} ({oracle_type} -> Feishu type {feishu_type})")
        
        # Create the table with all fields
        # 创建包含所有字段的表
        table_id = self.create_table(table_name, fields)
        logger.info(f"根据Oracle表架构创建飞书表 / Created Feishu table from Oracle schema: {table_name} (ID: {table_id})")
        
        return table_id
    
    def list_tables(self) -> List[Dict[str, Any]]:
        """
        List all tables in the bitable using SDK
        使用SDK列出多维表格中的所有表
        
        Returns:
            List of tables / 表列表
        """
        self._apply_rate_limit()  # 应用速率限制
        
        tables = []
        page_token = None
        
        # 分页获取所有表
        while True:
            request_builder = ListAppTableRequest.builder() \
                .app_token(self.app_token) \
                .page_size(100)
            
            if page_token:
                request_builder.page_token(page_token)
            
            request = request_builder.build()
            response = self.client.bitable.v1.app_table.list(request)
            
            if not response.success():
                logger.error(f"列出表失败 / Failed to list tables: {response.code}, {response.msg}")
                raise Exception(f"Failed to list tables: {response.msg}")
            
            # 收集表信息
            if response.data.items:
                for item in response.data.items:
                    tables.append({
                        "table_id": item.table_id,
                        "name": item.name,
                        "revision": item.revision
                    })
            
            # 检查是否还有更多数据
            if not response.data.has_more:
                break
            page_token = response.data.page_token
        
        return tables
    
    def get_table_fields(self, table_id: str) -> List[Dict[str, Any]]:
        """
        Get field definitions for a table using SDK
        使用SDK获取表的字段定义
        
        Args:
            table_id: Table ID / 表ID
            
        Returns:
            List of field definitions / 字段定义列表
        """
        self._apply_rate_limit()  # 应用速率限制
        
        # 构建获取字段列表的请求
        request = ListAppTableFieldRequest.builder() \
            .app_token(self.app_token) \
            .table_id(table_id) \
            .page_size(100) \
            .build()
        
        response = self.client.bitable.v1.app_table_field.list(request)
        
        if not response.success():
            logger.error(f"获取字段失败 / Failed to get fields: {response.code}, {response.msg}")
            raise Exception(f"Failed to get fields: {response.msg}")
        
        # 收集字段信息
        fields = []
        if response.data.items:
            for item in response.data.items:
                fields.append({
                    "field_id": item.field_id,
                    "field_name": item.field_name,
                    "type": item.type
                })
        
        return fields
    
    def create_fields_if_needed(self, table_id: str, sample_record: Dict[str, Any]):
        """
        Create fields in table based on sample record using SDK
        使用SDK根据样本记录在表中创建字段
        
        Args:
            table_id: Table ID / 表ID
            sample_record: Sample record to infer field types / 用于推断字段类型的样本记录
        """
        # 获取现有字段
        existing_fields = self.get_table_fields(table_id)
        existing_field_names = {field['field_name'] for field in existing_fields}
        
        # 创建缺失的字段
        for field_name, value in sample_record.items():
            if field_name not in existing_field_names:
                self._apply_rate_limit()  # 应用速率限制
                
                # 从值推断字段类型
                field_type = self._infer_field_type(value)
                
                # 构建创建字段的请求
                request = CreateAppTableFieldRequest.builder() \
                    .app_token(self.app_token) \
                    .table_id(table_id) \
                    .request_body(AppTableField.builder()
                        .field_name(field_name)
                        .type(field_type)
                        .build()) \
                    .build()
                
                response = self.client.bitable.v1.app_table_field.create(request)
                
                if not response.success():
                    logger.warning(f"创建字段失败 / Failed to create field {field_name}: {response.code}, {response.msg}")
                else:
                    logger.info(f"创建字段 / Created field: {field_name} (type: {field_type})")
    
    def _infer_field_type(self, value: Any) -> int:
        """
        Infer Feishu field type from value
        从值推断飞书字段类型
        
        Args:
            value: Sample value / 样本值
            
        Returns:
            Field type code / 字段类型代码
        """
        if value is None:
            return FIELD_TYPE_TEXT
        elif isinstance(value, bool):
            return FIELD_TYPE_CHECKBOX
        elif isinstance(value, (int, float)):
            return FIELD_TYPE_NUMBER
        elif isinstance(value, str):
            # Try to detect date format using proper parsing
            # 尝试使用正确的解析来检测日期格式
            if len(value) >= 10:
                try:
                    from datetime import datetime
                    # Try ISO format parsing
                    datetime.fromisoformat(value.split('T')[0])
                    return FIELD_TYPE_DATE
                except (ValueError, AttributeError):
                    pass
            return FIELD_TYPE_TEXT
        else:
            return FIELD_TYPE_TEXT
    
    def map_oracle_type_to_feishu(self, oracle_type: str) -> int:
        """
        Map Oracle data type to Feishu field type
        将Oracle数据类型映射到飞书字段类型
        
        Args:
            oracle_type: Oracle data type (e.g., 'VARCHAR2', 'NUMBER', 'DATE') / Oracle数据类型
            
        Returns:
            Feishu field type code / 飞书字段类型代码
        """
        # 转换为大写以进行匹配
        oracle_type_upper = oracle_type.upper()
        
        # 数字类型映射
        if oracle_type_upper in ('NUMBER', 'INTEGER', 'FLOAT', 'BINARY_FLOAT', 'BINARY_DOUBLE', 'DECIMAL', 'NUMERIC'):
            return FIELD_TYPE_NUMBER
        
        # 日期时间类型映射
        elif oracle_type_upper in ('DATE', 'TIMESTAMP', 'TIMESTAMP(6)', 'TIMESTAMP WITH TIME ZONE', 'TIMESTAMP WITH LOCAL TIME ZONE'):
            return FIELD_TYPE_DATE
        
        # 文本类型映射
        elif oracle_type_upper in ('VARCHAR', 'VARCHAR2', 'CHAR', 'NCHAR', 'NVARCHAR2', 'CLOB', 'NCLOB', 'LONG'):
            return FIELD_TYPE_TEXT
        
        # 默认为文本类型
        else:
            logger.warning(f"未知的Oracle类型 '{oracle_type}'，默认映射为文本类型 / Unknown Oracle type '{oracle_type}', defaulting to TEXT")
            return FIELD_TYPE_TEXT
    
    def batch_create_records(
        self, 
        table_id: str, 
        records: List[Dict[str, Any]]
    ) -> List[str]:
        """
        Batch create records in table using SDK (max 500 per batch)
        使用SDK批量创建表记录（每批最多500条）
        
        Args:
            table_id: Table ID / 表ID
            records: List of records to create (max 500) / 要创建的记录列表（最多500条）
            
        Returns:
            List of created record IDs / 已创建记录的ID列表
        """
        if len(records) > 500:
            raise ValueError("批次大小不能超过500条记录 / Batch size cannot exceed 500 records")
        
        self._apply_rate_limit()  # 应用速率限制
        
        # 将记录格式化为SDK格式
        sdk_records = []
        for record in records:
            app_table_record = AppTableRecord.builder() \
                .fields(record) \
                .build()
            sdk_records.append(app_table_record)
        
        # 构建批量创建记录的请求
        request = BatchCreateAppTableRecordRequest.builder() \
            .app_token(self.app_token) \
            .table_id(table_id) \
            .request_body(BatchCreateAppTableRecordRequestBody.builder()
                .records(sdk_records)
                .build()) \
            .build()
        
        response = self.client.bitable.v1.app_table_record.batch_create(request)
        
        if not response.success():
            logger.error(f"创建记录失败 / Failed to create records: {response.code}, {response.msg}")
            raise Exception(f"Failed to create records: {response.msg}")
        
        # 提取记录ID
        record_ids = []
        if response.data.records:
            record_ids = [record.record_id for record in response.data.records]
        
        logger.info(f"在表 {table_id} 中创建了 {len(record_ids)} 条记录 / Created {len(record_ids)} records in table {table_id}")
        return record_ids
    
    def get_or_create_next_table(
        self, 
        sample_record: Dict[str, Any],
        sequence_number: int
    ) -> str:
        """
        Get existing table or create new one if current is full
        获取现有表，如果当前表已满则创建新表
        
        Args:
            sample_record: Sample record to create fields / 用于创建字段的样本记录
            sequence_number: Sequence number for new table name / 新表名的序号
            
        Returns:
            Table ID to use / 要使用的表ID
        """
        # 检查当前表是否已满
        if self.current_table_row_count >= self.max_rows_per_table:
            # 创建新表，表名包含序号（如：DataSync_001）
            table_name = f"{self.table_name_prefix}_{sequence_number:03d}"
            
            # 根据样本记录创建基本字段
            fields = []
            for field_name, value in sample_record.items():
                field_type = self._infer_field_type(value)
                fields.append({
                    "field_name": field_name,
                    "type": field_type
                })
            
            # 创建新表
            new_table_id = self.create_table(table_name, fields)
            self.current_table_id = new_table_id
            self.current_table_row_count = 0
            
            return new_table_id
        
        return self.current_table_id
    
    def write_records_with_table_management(
        self,
        records: List[Dict[str, Any]],
        table_sequence: int,
        oracle_schema: List[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Write records with automatic table switching when limit reached
        写入记录，当达到限制时自动切换表
        
        Args:
            records: Records to write / 要写入的记录
            table_sequence: Current table sequence number / 当前表序号
            oracle_schema: Oracle table schema for field mapping / 用于字段映射的Oracle表架构
            
        Returns:
            Result summary with table_id and new sequence number / 包含表ID和新序号的结果摘要
        """
        if not records:
            return {"table_id": self.current_table_id, "sequence": table_sequence, "written": 0}
        
        total_written = 0
        current_sequence = table_sequence
        
        # Ensure table exists (auto-create if base_table_id not provided)
        # 确保表存在（如果未提供base_table_id则自动创建）
        if self.current_table_id is None:
            if oracle_schema:
                # Create table based on Oracle schema
                # 根据Oracle架构创建表
                self.current_table_id = self.create_table_from_oracle_schema(oracle_schema, current_sequence)
                self.current_table_row_count = 0
            else:
                # Fallback to sample-based creation
                # 回退到基于样本的创建
                self.ensure_table_exists(records[0], current_sequence)
        
        # 更新当前表行数
        self.current_table_row_count = self.get_table_row_count(self.current_table_id)
        
        # 分批处理记录（每批500条，飞书API限制）
        batch_size = 500
        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]
            
            # 检查是否需要创建新表
            if self.current_table_row_count + len(batch) > self.max_rows_per_table:
                # 创建新表
                current_sequence += 1
                if oracle_schema:
                    # Use Oracle schema for new table
                    # 使用Oracle架构创建新表
                    self.current_table_id = self.create_table_from_oracle_schema(oracle_schema, current_sequence)
                    self.current_table_row_count = 0
                else:
                    # Fallback to existing method
                    # 回退到现有方法
                    self.get_or_create_next_table(records[0], current_sequence)
            
            # 确保当前表中存在所需字段（首批或新表）
            if i == 0 or self.current_table_row_count == 0:
                self.create_fields_if_needed(self.current_table_id, records[0])
            
            # 写入批次数据
            self.batch_create_records(self.current_table_id, batch)
            self.current_table_row_count += len(batch)
            total_written += len(batch)
        
        return {
            "table_id": self.current_table_id,
            "sequence": current_sequence,
            "written": total_written
        }
