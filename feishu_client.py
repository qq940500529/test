"""
Feishu (Lark) API Client
Handles interaction with Feishu multi-dimensional tables (Bitable)
Uses official Feishu SDK (lark-oapi)
"""
import time
import logging
from typing import List, Dict, Any, Optional
from ratelimit import limits, sleep_and_retry
import lark_oapi as lark
from lark_oapi.api.bitable.v1 import *

logger = logging.getLogger(__name__)


class FeishuClient:
    """Feishu API client with rate limiting support using official SDK"""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize Feishu client with SDK
        
        Args:
            config: Feishu configuration dictionary
        """
        self.app_id = config['app_id']
        self.app_secret = config['app_secret']
        self.app_token = config['app_token']
        self.base_table_id = config['base_table_id']
        self.table_name_prefix = config.get('table_name_prefix', 'DataSync')
        self.max_rows_per_table = config.get('max_rows_per_table', 20000)
        self.max_requests_per_second = config.get('max_requests_per_second', 50)
        
        # Initialize Feishu SDK client
        self.client = lark.Client.builder() \
            .app_id(self.app_id) \
            .app_secret(self.app_secret) \
            .log_level(lark.LogLevel.INFO) \
            .build()
        
        # Track current table info
        self.current_table_id = self.base_table_id
        self.current_table_row_count = 0
        
    @sleep_and_retry
    @limits(calls=50, period=1)  # 50 requests per second
    def _rate_limited_call(self):
        """Rate limiting decorator for API calls"""
        pass
    
    def get_table_info(self, table_id: str) -> Dict[str, Any]:
        """
        Get table information using SDK
        
        Args:
            table_id: Table ID
            
        Returns:
            Table information
        """
        self._rate_limited_call()
        
        request = GetAppTableRequest.builder() \
            .app_token(self.app_token) \
            .table_id(table_id) \
            .build()
        
        response = self.client.bitable.v1.app_table.get(request)
        
        if not response.success():
            logger.error(f"Failed to get table info: {response.code}, {response.msg}")
            raise Exception(f"Failed to get table info: {response.msg}")
        
        return {
            "table_id": response.data.table.table_id,
            "name": response.data.table.name,
            "revision": response.data.table.revision
        }
    
    def get_table_row_count(self, table_id: str) -> int:
        """
        Get current row count in a table using SDK
        
        Args:
            table_id: Table ID
            
        Returns:
            Number of rows
        """
        self._rate_limited_call()
        
        request = ListAppTableRecordRequest.builder() \
            .app_token(self.app_token) \
            .table_id(table_id) \
            .page_size(1) \
            .build()
        
        response = self.client.bitable.v1.app_table_record.list(request)
        
        if not response.success():
            logger.error(f"Failed to get row count: {response.code}, {response.msg}")
            raise Exception(f"Failed to get row count: {response.msg}")
        
        return response.data.total if response.data.total else 0
    
    def create_table(self, table_name: str, fields: List[Dict[str, Any]]) -> str:
        """
        Create a new table in the bitable using SDK
        
        Args:
            table_name: Name for the new table
            fields: List of field definitions
            
        Returns:
            New table ID
        """
        self._rate_limited_call()
        
        # Convert fields to SDK format
        sdk_fields = []
        for field in fields:
            app_table_create_header = AppTableCreateHeader.builder() \
                .field_name(field['field_name']) \
                .type(field['type']) \
                .build()
            sdk_fields.append(app_table_create_header)
        
        request = CreateAppTableRequest.builder() \
            .app_token(self.app_token) \
            .request_body(ReqAppTable.builder()
                .table(ReqAppTableTable.builder()
                    .name(table_name)
                    .default_view_name("Default View")
                    .fields(sdk_fields)
                    .build())
                .build()) \
            .build()
        
        response = self.client.bitable.v1.app_table.create(request)
        
        if not response.success():
            logger.error(f"Failed to create table: {response.code}, {response.msg}")
            raise Exception(f"Failed to create table: {response.msg}")
        
        table_id = response.data.table_id
        logger.info(f"Created new table: {table_name} (ID: {table_id})")
        return table_id
    
    def list_tables(self) -> List[Dict[str, Any]]:
        """
        List all tables in the bitable using SDK
        
        Returns:
            List of tables
        """
        self._rate_limited_call()
        
        tables = []
        page_token = None
        
        while True:
            request_builder = ListAppTableRequest.builder() \
                .app_token(self.app_token) \
                .page_size(100)
            
            if page_token:
                request_builder.page_token(page_token)
            
            request = request_builder.build()
            response = self.client.bitable.v1.app_table.list(request)
            
            if not response.success():
                logger.error(f"Failed to list tables: {response.code}, {response.msg}")
                raise Exception(f"Failed to list tables: {response.msg}")
            
            if response.data.items:
                for item in response.data.items:
                    tables.append({
                        "table_id": item.table_id,
                        "name": item.name,
                        "revision": item.revision
                    })
            
            if not response.data.has_more:
                break
            page_token = response.data.page_token
        
        return tables
    
    def get_table_fields(self, table_id: str) -> List[Dict[str, Any]]:
        """
        Get field definitions for a table using SDK
        
        Args:
            table_id: Table ID
            
        Returns:
            List of field definitions
        """
        self._rate_limited_call()
        
        request = ListAppTableFieldRequest.builder() \
            .app_token(self.app_token) \
            .table_id(table_id) \
            .page_size(100) \
            .build()
        
        response = self.client.bitable.v1.app_table_field.list(request)
        
        if not response.success():
            logger.error(f"Failed to get fields: {response.code}, {response.msg}")
            raise Exception(f"Failed to get fields: {response.msg}")
        
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
        
        Args:
            table_id: Table ID
            sample_record: Sample record to infer field types
        """
        # Get existing fields
        existing_fields = self.get_table_fields(table_id)
        existing_field_names = {field['field_name'] for field in existing_fields}
        
        # Create missing fields
        for field_name, value in sample_record.items():
            if field_name not in existing_field_names:
                self._rate_limited_call()
                
                # Infer field type from value
                field_type = self._infer_field_type(value)
                
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
                    logger.warning(f"Failed to create field {field_name}: {response.code}, {response.msg}")
                else:
                    logger.info(f"Created field: {field_name} (type: {field_type})")
    
    def _infer_field_type(self, value: Any) -> int:
        """
        Infer Feishu field type from value
        
        Args:
            value: Sample value
            
        Returns:
            Field type code
        """
        # Feishu field types:
        # 1: Text, 2: Number, 3: Single Select, 4: Multi Select
        # 5: Date, 7: Checkbox, 11: Person, 13: Phone, 15: URL
        # 17: Attachment, 18: Single Link, 20: Formula, 21: Two-way Link
        # 22: Location, 23: GroupChat, 1001: Created time, 1002: Modified time
        # 1003: Created by, 1004: Modified by, 1005: Auto number
        
        if value is None:
            return 1  # Text
        elif isinstance(value, bool):
            return 7  # Checkbox
        elif isinstance(value, (int, float)):
            return 2  # Number
        elif isinstance(value, str):
            # Try to detect date format
            if len(value) >= 10 and value[4] == '-' and value[7] == '-':
                return 5  # Date
            return 1  # Text
        else:
            return 1  # Default to text
    
    def batch_create_records(
        self, 
        table_id: str, 
        records: List[Dict[str, Any]]
    ) -> List[str]:
        """
        Batch create records in table using SDK (max 500 per batch)
        
        Args:
            table_id: Table ID
            records: List of records to create (max 500)
            
        Returns:
            List of created record IDs
        """
        if len(records) > 500:
            raise ValueError("Batch size cannot exceed 500 records")
        
        self._rate_limited_call()
        
        # Format records for SDK
        sdk_records = []
        for record in records:
            app_table_record = AppTableRecord.builder() \
                .fields(record) \
                .build()
            sdk_records.append(app_table_record)
        
        request = BatchCreateAppTableRecordRequest.builder() \
            .app_token(self.app_token) \
            .table_id(table_id) \
            .request_body(BatchCreateAppTableRecordRequestBody.builder()
                .records(sdk_records)
                .build()) \
            .build()
        
        response = self.client.bitable.v1.app_table_record.batch_create(request)
        
        if not response.success():
            logger.error(f"Failed to create records: {response.code}, {response.msg}")
            raise Exception(f"Failed to create records: {response.msg}")
        
        record_ids = []
        if response.data.records:
            record_ids = [record.record_id for record in response.data.records]
        
        logger.info(f"Created {len(record_ids)} records in table {table_id}")
        return record_ids
    
    def get_or_create_next_table(
        self, 
        sample_record: Dict[str, Any],
        sequence_number: int
    ) -> str:
        """
        Get existing table or create new one if current is full
        
        Args:
            sample_record: Sample record to create fields
            sequence_number: Sequence number for new table name
            
        Returns:
            Table ID to use
        """
        # Check if current table is full
        if self.current_table_row_count >= self.max_rows_per_table:
            # Create new table
            table_name = f"{self.table_name_prefix}_{sequence_number:03d}"
            
            # Create basic fields from sample record
            fields = []
            for field_name, value in sample_record.items():
                field_type = self._infer_field_type(value)
                fields.append({
                    "field_name": field_name,
                    "type": field_type
                })
            
            new_table_id = self.create_table(table_name, fields)
            self.current_table_id = new_table_id
            self.current_table_row_count = 0
            
            return new_table_id
        
        return self.current_table_id
    
    def write_records_with_table_management(
        self,
        records: List[Dict[str, Any]],
        table_sequence: int
    ) -> Dict[str, Any]:
        """
        Write records with automatic table switching when limit reached
        
        Args:
            records: Records to write
            table_sequence: Current table sequence number
            
        Returns:
            Result summary with table_id and new sequence number
        """
        if not records:
            return {"table_id": self.current_table_id, "sequence": table_sequence, "written": 0}
        
        total_written = 0
        current_sequence = table_sequence
        
        # Update current table row count
        self.current_table_row_count = self.get_table_row_count(self.current_table_id)
        
        # Process records in batches
        batch_size = 500  # Feishu API limit
        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]
            
            # Check if we need a new table
            if self.current_table_row_count + len(batch) > self.max_rows_per_table:
                # Create new table
                current_sequence += 1
                self.get_or_create_next_table(records[0], current_sequence)
            
            # Ensure fields exist in current table
            if i == 0 or self.current_table_row_count == 0:
                self.create_fields_if_needed(self.current_table_id, records[0])
            
            # Write batch
            self.batch_create_records(self.current_table_id, batch)
            self.current_table_row_count += len(batch)
            total_written += len(batch)
        
        return {
            "table_id": self.current_table_id,
            "sequence": current_sequence,
            "written": total_written
        }
