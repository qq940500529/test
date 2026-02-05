"""
Oracle Database Module
Oracle数据库模块

Handles connection and data reading from Oracle database
处理与Oracle数据库的连接和数据读取
"""
import cx_Oracle
import logging
from typing import List, Dict, Any, Optional

logger = logging.getLogger(__name__)


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
        # 查询表的列信息，按列顺序排序
        query = f"""
            SELECT column_name 
            FROM user_tab_columns 
            WHERE table_name = UPPER('{table_name}')
            ORDER BY column_id
        """
        self.cursor.execute(query)
        columns = [row[0] for row in self.cursor.fetchall()]
        logger.info(f"在表 {table_name} 中找到 {len(columns)} 列 / Found {len(columns)} columns in table {table_name}")
        return columns
    
    def get_total_count(self, table_name: str, where_clause: str = "") -> int:
        """
        Get total record count
        获取记录总数
        
        Args:
            table_name: Name of the table / 表名
            where_clause: Optional WHERE clause for filtering / 可选的WHERE条件子句
            
        Returns:
            Total count of records / 记录总数
        """
        # 构建COUNT查询语句
        query = f"SELECT COUNT(*) FROM {table_name}"
        if where_clause:
            query += f" WHERE {where_clause}"  # 添加过滤条件
        
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
        where_clause: str = "",
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
            where_clause: Optional WHERE clause / 可选WHERE条件
            order_by: Optional ORDER BY clause / 可选ORDER BY子句
            
        Returns:
            List of records as dictionaries / 记录列表（字典格式）
        """
        # 构建SELECT语句的列部分
        columns_str = ", ".join(columns)
        query = f"SELECT {columns_str} FROM {table_name}"
        
        # 添加WHERE条件（如果有）
        if where_clause:
            query += f" WHERE {where_clause}"
        
        # 添加排序（如果有）
        if order_by:
            query += f" ORDER BY {order_by}"
        
        # Oracle分页查询：使用ROWNUM实现分页
        # 内层查询限制最大行数，外层查询过滤掉之前的行
        query = f"""
            SELECT * FROM (
                SELECT a.*, ROWNUM rnum FROM (
                    {query}
                ) a WHERE ROWNUM <= {offset + batch_size}
            ) WHERE rnum > {offset}
        """
        
        self.cursor.execute(query)
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
