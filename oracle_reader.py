"""
Oracle Database Module
Oracle数据库模块

Handles connection and data reading from Oracle database
处理与Oracle数据库的连接和数据读取
"""
import cx_Oracle
import logging
import re
from typing import List, Dict, Any, Optional

logger = logging.getLogger(__name__)


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
        
    def connect(self):
        """
        Establish connection to Oracle database
        建立与Oracle数据库的连接
        """
        try:
            # 构建数据源名称(DSN)
            dsn = cx_Oracle.makedsn(
                self.config['host'],  # 数据库主机地址
                self.config['port'],  # 数据库端口
                service_name=self.config['service_name']  # 服务名
            )
            # 创建数据库连接
            self.connection = cx_Oracle.connect(
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
            # 对于增量同步，使用参数化查询
            query = f"SELECT COUNT(*) FROM {table_name} WHERE {sync_column} > :last_value"
            self.cursor.execute(query, last_value=last_sync_value)
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
            base_query += f" WHERE {sync_column} > :last_value"
            params['last_value'] = last_sync_value
        
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
                if isinstance(value, cx_Oracle.LOB):
                    value = value.read()  # LOB类型读取为文本
                elif hasattr(value, 'isoformat'):  # 日期时间对象
                    value = value.isoformat()  # 转换为ISO格式字符串
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
