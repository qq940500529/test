"""
Oracle Database Module
Oracle数据库模块

Handles connection and data reading from Oracle database
处理与Oracle数据库的连接和数据读取
"""
import oracledb
import logging
import re
from typing import List, Dict, Any, Optional
from datetime import datetime, timezone, timedelta

logger = logging.getLogger(__name__)

# 东八区时区 (+8小时)
TIMEZONE_UTC8 = timezone(timedelta(hours=8))

# Oracle日期/时间戳类型常量
# Oracle DATE/TIMESTAMP types constants
_DATE_TYPES = ('DATE', 'TIMESTAMP')


def _validate_sql_identifier(identifier: str) -> bool:
    """
    Validate SQL identifier to prevent SQL injection
    验证SQL标识符以防止SQL注入
    
    Args:
        identifier: SQL identifier (table name, column name, etc.) / SQL标识符
        
    Returns:
        True if valid, raises ValueError if invalid
    """
    # Allow alphanumeric characters, underscores, and dollar signs
    # 允许字母数字字符、下划线和美元符号
    if not re.match(r'^[a-zA-Z][a-zA-Z0-9_$]*$', identifier):
        raise ValueError(f"Invalid SQL identifier: {identifier}. Only alphanumeric characters, underscores, and dollar signs are allowed.")
    if len(identifier) > 30:  # Oracle limit
        raise ValueError(f"SQL identifier too long: {identifier}. Maximum length is 30 characters.")
    return True


class OracleDataReader:
    """
    Oracle database reader with batch processing support
    Oracle数据库读取器，支持批量处理
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize Oracle connection
        初始化Oracle连接
        
        Args:
            config: Oracle configuration dictionary / Oracle配置字典
        """
        self.config = config
        self.connection = None  # 数据库连接对象
        self.cursor = None  # 游标对象
        # 是否启用UTC到+8时区转换（默认启用）
        self.convert_utc_to_utc8 = config.get('convert_utc_to_utc8', True)
        # 缓存表的架构信息（用于类型检查）
        self._table_schemas = {}  # {table_name: {column_name: data_type}}
        
    def connect(self):
        """
        Establish connection to Oracle database
        建立与Oracle数据库的连接
        """
        try:
            # 构建数据源名称(DSN)
            dsn = oracledb.makedsn(
                self.config['host'],  # 数据库主机地址
                self.config['port'],  # 数据库端口
                service_name=self.config['service_name']  # 服务名
            )
            # 创建数据库连接
            self.connection = oracledb.connect(
                user=self.config['username'],  # 用户名
                password=self.config['password'],  # 密码
                dsn=dsn
            )
            # 创建游标用于执行SQL语句
            self.cursor = self.connection.cursor()
            logger.info("成功连接到Oracle数据库 / Successfully connected to Oracle database")
        except Exception as e:
            logger.error(f"连接Oracle失败 / Failed to connect to Oracle: {e}")
            raise
    
    def disconnect(self):
        """
        Close database connection
        关闭数据库连接
        """
        if self.cursor:
            self.cursor.close()  # 关闭游标
        if self.connection:
            self.connection.close()  # 关闭连接
        logger.info("已断开Oracle数据库连接 / Disconnected from Oracle database")
    
    def convert_utc_datetime_to_utc8(self, dt: datetime) -> datetime:
        """
        Convert UTC datetime to UTC+8 timezone
        将UTC时间转换为东八区时间（+8小时）
        
        Args:
            dt: UTC datetime object / UTC时间对象
            
        Returns:
            Datetime in UTC+8 timezone / 东八区时间对象
            
        Note:
            If the datetime object has no timezone info (tzinfo is None), 
            it is assumed to be in UTC timezone.
            如果时间对象没有时区信息（tzinfo为None），则假定为UTC时区。
        """
        if dt is None:
            return None
        
        # 如果时间对象没有时区信息，假定为UTC
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        
        # 转换到东八区
        dt_utc8 = dt.astimezone(TIMEZONE_UTC8)
        
        return dt_utc8
    
    def _get_column_type(self, table_name: str, column_name: str) -> Optional[str]:
        """
        Get the data type of a column
        获取列的数据类型
        
        Args:
            table_name: Name of the table / 表名
            column_name: Name of the column / 列名
            
        Returns:
            Data type string or None if not found / 数据类型字符串，如果未找到则为None
        """
        if table_name not in self._table_schemas:
            # Schema not cached, fetch it
            # 架构未缓存，获取它
            self.get_table_schema(table_name)
        
        return self._table_schemas.get(table_name, {}).get(column_name)
    
    def _convert_timestamp_to_date(self, timestamp_ms: int) -> datetime:
        """
        Convert millisecond timestamp to datetime object
        将毫秒时间戳转换为datetime对象
        
        Args:
            timestamp_ms: Timestamp in milliseconds / 毫秒时间戳
            
        Returns:
            Datetime object / datetime对象
        """
        # Convert milliseconds to seconds
        # 将毫秒转换为秒
        timestamp_sec = timestamp_ms / 1000
        return datetime.fromtimestamp(timestamp_sec, tz=timezone.utc)
    
    def _prepare_sync_value_for_query(self, table_name: str, sync_column: str, last_sync_value: Any) -> Any:
        """
        Prepare sync value for use in Oracle query
        准备用于Oracle查询的同步值
        
        If the sync column is a DATE type and the value is a numeric timestamp,
        convert it to a datetime object for proper Oracle comparison.
        如果同步列是DATE类型且值是数字时间戳，则将其转换为datetime对象以便在Oracle中正确比较。
        
        Args:
            table_name: Name of the table / 表名
            sync_column: Name of the sync column / 同步列名
            last_sync_value: The value to prepare / 要准备的值
            
        Returns:
            Prepared value for Oracle query / 准备好用于Oracle查询的值
        """
        if last_sync_value is None:
            return None
        
        # Get the column type
        # 获取列类型
        column_type = self._get_column_type(table_name, sync_column)
        
        # If it's a DATE or TIMESTAMP type and the value is numeric (millisecond timestamp)
        # 如果是DATE或TIMESTAMP类型且值是数字（毫秒时间戳）
        # Check if column_type is DATE or starts with TIMESTAMP to handle all precision variants
        # 检查是否为DATE或以TIMESTAMP开头，以处理所有精度变体
        is_date_type = column_type in _DATE_TYPES or (column_type and column_type.startswith('TIMESTAMP'))
        
        if is_date_type and isinstance(last_sync_value, (int, float)):
            # Convert millisecond timestamp to datetime
            # 将毫秒时间戳转换为datetime
            return self._convert_timestamp_to_date(last_sync_value)
        
        return last_sync_value
    
    def get_table_columns(self, table_name: str) -> List[str]:
        """
        Get column names from table
        获取表的列名
        
        Args:
            table_name: Name of the table / 表名
            
        Returns:
            List of column names / 列名列表
        """
        # 验证表名以防止SQL注入
        _validate_sql_identifier(table_name)
        
        # 使用参数化查询防止SQL注入
        # Use parameterized query to prevent SQL injection
        query = """
            SELECT column_name 
            FROM user_tab_columns 
            WHERE table_name = UPPER(:table_name)
            ORDER BY column_id
        """
        self.cursor.execute(query, table_name=table_name)
        columns = [row[0] for row in self.cursor.fetchall()]
        logger.info(f"在表 {table_name} 中找到 {len(columns)} 列 / Found {len(columns)} columns in table {table_name}")
        return columns
    
    def get_table_schema(self, table_name: str) -> List[Dict[str, Any]]:
        """
        Get column schema (names and types) from table
        获取表的字段架构（名称和类型）
        
        Args:
            table_name: Name of the table / 表名
            
        Returns:
            List of column definitions with name and data_type / 包含名称和数据类型的列定义列表
        """
        # 验证表名以防止SQL注入
        _validate_sql_identifier(table_name)
        
        # 使用参数化查询获取字段信息
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
                'column_name': row[0],
                'data_type': row[1],
                'data_length': row[2],
                'data_precision': row[3],
                'data_scale': row[4]
            }
            columns_info.append(column_info)
        
        logger.info(f"获取到表 {table_name} 的 {len(columns_info)} 个字段架构 / Retrieved schema for {len(columns_info)} columns in table {table_name}")
        
        # 缓存架构信息以便快速查询列类型
        # Cache schema information for quick column type lookup
        self._table_schemas[table_name] = {
            col['column_name']: col['data_type'] for col in columns_info
        }
        
        return columns_info
    
    def get_total_count(self, table_name: str, sync_column: str = None, last_sync_value: Any = None) -> int:
        """
        Get total record count
        获取记录总数
        
        Args:
            table_name: Name of the table / 表名
            sync_column: Column for incremental sync / 增量同步列
            last_sync_value: Last sync value for filtering / 用于过滤的最后同步值
            
        Returns:
            Total count of records / 记录总数
        """
        # 验证SQL标识符以防止SQL注入
        _validate_sql_identifier(table_name)
        if sync_column:
            _validate_sql_identifier(sync_column)
        
        # 使用参数化查询防止SQL注入
        # Use parameterized query to prevent SQL injection
        if sync_column and last_sync_value is not None:
            # 对于增量同步，准备同步值（如果是DATE类型则转换时间戳）
            # For incremental sync, prepare sync value (convert timestamp if DATE type)
            prepared_value = self._prepare_sync_value_for_query(table_name, sync_column, last_sync_value)
            query = f"SELECT COUNT(*) FROM {table_name} WHERE {sync_column} > :last_value"
            self.cursor.execute(query, last_value=prepared_value)
        else:
            query = f"SELECT COUNT(*) FROM {table_name}"
            self.cursor.execute(query)
        
        count = self.cursor.fetchone()[0]
        logger.info(f"记录总数 / Total records: {count}")
        return count
    
    def read_batch(
        self, 
        table_name: str, 
        columns: List[str],
        batch_size: int = 1000,
        offset: int = 0,
        sync_column: str = None,
        last_sync_value: Any = None,
        order_by: str = ""
    ) -> List[Dict[str, Any]]:
        """
        Read a batch of records from Oracle
        从Oracle批量读取记录
        
        Args:
            table_name: Name of the table / 表名
            columns: List of column names to read / 要读取的列名列表
            batch_size: Number of records per batch / 每批记录数
            offset: Starting offset for pagination / 分页起始偏移量
            sync_column: Column for incremental sync / 增量同步列
            last_sync_value: Last sync value for filtering / 用于过滤的最后同步值
            order_by: Optional ORDER BY clause / 可选ORDER BY子句
            
        Returns:
            List of records as dictionaries / 记录列表（字典格式）
        """
        # 验证SQL标识符以防止SQL注入
        _validate_sql_identifier(table_name)
        for col in columns:
            _validate_sql_identifier(col)
        if sync_column:
            _validate_sql_identifier(sync_column)
        if order_by:
            _validate_sql_identifier(order_by)
        
        # 构建SELECT语句的列部分
        columns_str = ", ".join(columns)
        base_query = f"SELECT {columns_str} FROM {table_name}"
        
        # 构建WHERE条件（使用参数化查询）
        params = {}
        if sync_column and last_sync_value is not None:
            # 准备同步值（如果是DATE类型则转换时间戳）
            # Prepare sync value (convert timestamp if DATE type)
            prepared_value = self._prepare_sync_value_for_query(table_name, sync_column, last_sync_value)
            base_query += f" WHERE {sync_column} > :last_value"
            params['last_value'] = prepared_value
        
        # 添加排序（如果有）
        if order_by:
            base_query += f" ORDER BY {order_by}"
        
        # Oracle分页查询：使用ROWNUM实现分页
        # 内层查询限制最大行数，外层查询过滤掉之前的行
        query = f"""
            SELECT * FROM (
                SELECT a.*, ROWNUM rnum FROM (
                    {base_query}
                ) a WHERE ROWNUM <= :max_row
            ) WHERE rnum > :min_row
        """
        
        params['max_row'] = offset + batch_size
        params['min_row'] = offset
        
        self.cursor.execute(query, **params)
        rows = self.cursor.fetchall()
        
        # 将查询结果转换为字典列表
        records = []
        for row in rows:
            record = {}
            for i, col in enumerate(columns):
                value = row[i]
                # 转换Oracle特定类型为JSON可序列化类型
                if isinstance(value, oracledb.LOB):
                    value = value.read()  # LOB类型读取为文本
                elif hasattr(value, 'isoformat'):  # 日期时间对象
                    # 如果启用UTC到+8时区转换
                    if self.convert_utc_to_utc8:
                        value = self.convert_utc_datetime_to_utc8(value)
                    # 转换为毫秒级时间戳（飞书API要求）/ Convert to millisecond timestamp (required by Feishu API)
                    try:
                        value = int(value.timestamp() * 1000)
                    except (OSError, OverflowError) as e:
                        logger.warning(f"无法转换日期时间为时间戳 / Failed to convert datetime to timestamp: {value}, error: {e}")
                        # 对于超出范围的日期，设置为None
                        value = None
                record[col] = value
            records.append(record)
        
        logger.info(f"读取了 {len(records)} 条记录 (偏移量: {offset}) / Read {len(records)} records (offset: {offset})")
        return records
    
    def get_max_value(self, table_name: str, column_name: str) -> Any:
        """
        Get maximum value of a column (for checkpoint tracking)
        获取列的最大值（用于检查点跟踪）
        
        Args:
            table_name: Name of the table / 表名
            column_name: Column to get max value from / 要获取最大值的列名
            
        Returns:
            Maximum value / 最大值
        """
        # 验证SQL标识符以防止SQL注入
        _validate_sql_identifier(table_name)
        _validate_sql_identifier(column_name)
        
        query = f"SELECT MAX({column_name}) FROM {table_name}"
        self.cursor.execute(query)
        result = self.cursor.fetchone()[0]
        return result
    
    def __enter__(self):
        """
        Context manager entry
        上下文管理器入口：支持with语句
        """
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Context manager exit
        上下文管理器出口：自动清理资源
        """
        self.disconnect()
