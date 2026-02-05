"""
Oracle Database Module
Handles connection and data reading from Oracle database
"""
import cx_Oracle
import logging
from typing import List, Dict, Any, Optional

logger = logging.getLogger(__name__)


class OracleDataReader:
    """Oracle database reader with batch processing support"""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize Oracle connection
        
        Args:
            config: Oracle configuration dictionary
        """
        self.config = config
        self.connection = None
        self.cursor = None
        
    def connect(self):
        """Establish connection to Oracle database"""
        try:
            dsn = cx_Oracle.makedsn(
                self.config['host'],
                self.config['port'],
                service_name=self.config['service_name']
            )
            self.connection = cx_Oracle.connect(
                user=self.config['username'],
                password=self.config['password'],
                dsn=dsn
            )
            self.cursor = self.connection.cursor()
            logger.info("Successfully connected to Oracle database")
        except Exception as e:
            logger.error(f"Failed to connect to Oracle: {e}")
            raise
    
    def disconnect(self):
        """Close database connection"""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        logger.info("Disconnected from Oracle database")
    
    def get_table_columns(self, table_name: str) -> List[str]:
        """
        Get column names from table
        
        Args:
            table_name: Name of the table
            
        Returns:
            List of column names
        """
        query = f"""
            SELECT column_name 
            FROM user_tab_columns 
            WHERE table_name = UPPER('{table_name}')
            ORDER BY column_id
        """
        self.cursor.execute(query)
        columns = [row[0] for row in self.cursor.fetchall()]
        logger.info(f"Found {len(columns)} columns in table {table_name}")
        return columns
    
    def get_total_count(self, table_name: str, where_clause: str = "") -> int:
        """
        Get total record count
        
        Args:
            table_name: Name of the table
            where_clause: Optional WHERE clause for filtering
            
        Returns:
            Total count of records
        """
        query = f"SELECT COUNT(*) FROM {table_name}"
        if where_clause:
            query += f" WHERE {where_clause}"
        
        self.cursor.execute(query)
        count = self.cursor.fetchone()[0]
        logger.info(f"Total records: {count}")
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
        
        Args:
            table_name: Name of the table
            columns: List of column names to read
            batch_size: Number of records per batch
            offset: Starting offset for pagination
            where_clause: Optional WHERE clause
            order_by: Optional ORDER BY clause
            
        Returns:
            List of records as dictionaries
        """
        columns_str = ", ".join(columns)
        query = f"SELECT {columns_str} FROM {table_name}"
        
        if where_clause:
            query += f" WHERE {where_clause}"
        
        if order_by:
            query += f" ORDER BY {order_by}"
        
        # Oracle pagination
        query = f"""
            SELECT * FROM (
                SELECT a.*, ROWNUM rnum FROM (
                    {query}
                ) a WHERE ROWNUM <= {offset + batch_size}
            ) WHERE rnum > {offset}
        """
        
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        
        # Convert to list of dictionaries
        records = []
        for row in rows:
            record = {}
            for i, col in enumerate(columns):
                value = row[i]
                # Convert Oracle specific types to JSON serializable types
                if isinstance(value, cx_Oracle.LOB):
                    value = value.read()
                elif hasattr(value, 'isoformat'):  # datetime objects
                    value = value.isoformat()
                record[col] = value
            records.append(record)
        
        logger.info(f"Read {len(records)} records (offset: {offset})")
        return records
    
    def get_max_value(self, table_name: str, column_name: str) -> Any:
        """
        Get maximum value of a column (for checkpoint tracking)
        
        Args:
            table_name: Name of the table
            column_name: Column to get max value from
            
        Returns:
            Maximum value
        """
        query = f"SELECT MAX({column_name}) FROM {table_name}"
        self.cursor.execute(query)
        result = self.cursor.fetchone()[0]
        return result
    
    def __enter__(self):
        """Context manager entry"""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.disconnect()
