"""
Main Sync Program
主同步程序

Orchestrates data synchronization from Oracle to Feishu
协调Oracle到飞书的数据同步
"""
import yaml
import logging
import sys
from pathlib import Path
from typing import Dict, Any
from datetime import datetime

from oracle_reader import OracleDataReader
from feishu_client import FeishuClient
from checkpoint_manager import CheckpointManager

# 配置日志记录
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('sync.log'),  # 输出到文件
        logging.StreamHandler(sys.stdout)  # 输出到控制台
    ]
)

logger = logging.getLogger(__name__)


class OracleToFeishuSync:
    """
    Main synchronization orchestrator
    主同步协调器
    """
    
    def __init__(self, config_path: str = "config.yaml"):
        """
        Initialize sync orchestrator
        初始化同步协调器
        
        Args:
            config_path: Path to configuration file / 配置文件路径
        """
        self.config = self._load_config(config_path)  # 加载配置
        # 初始化检查点管理器
        self.checkpoint_manager = CheckpointManager(
            self.config['sync']['checkpoint_file']
        )
        self.oracle_reader = None  # Oracle读取器
        self.feishu_client = None  # 飞书客户端
    
    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """
        Load configuration from YAML file
        从YAML文件加载配置
        
        Args:
            config_path: Path to config file / 配置文件路径
            
        Returns:
            Configuration dictionary / 配置字典
        """
        config_file = Path(config_path)
        if not config_file.exists():
            raise FileNotFoundError(f"配置文件未找到 / Config file not found: {config_path}")
        
        with open(config_file, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        
        logger.info(f"从 {config_path} 加载配置 / Loaded configuration from {config_path}")
        return config
    
    def run(self, full_sync: bool = False):
        """
        Run synchronization
        运行同步
        
        Args:
            full_sync: If True, ignore checkpoint and sync all data / 如果为True，忽略检查点并同步所有数据
        """
        logger.info("=" * 60)
        logger.info("开始Oracle到飞书同步 / Starting Oracle to Feishu synchronization")
        logger.info("=" * 60)
        
        start_time = datetime.now()
        
        try:
            # 初始化连接
            self.oracle_reader = OracleDataReader(self.config['oracle'])
            self.feishu_client = FeishuClient(self.config['feishu'])
            
            # 连接到Oracle
            self.oracle_reader.connect()
            
            # 获取检查点数据
            checkpoint = self.checkpoint_manager.get_checkpoint_data()
            
            if full_sync:
                logger.info("完整同步模式：忽略检查点 / Full sync mode: ignoring checkpoint")
                checkpoint = self.checkpoint_manager.get_fresh_checkpoint()
                self.checkpoint_manager.reset()
            
            # 获取表信息
            table_name = self.config['oracle']['table_name']  # 表名
            sync_column = self.config['oracle']['sync_column']  # 同步列（用于增量）
            primary_key = self.config['oracle']['primary_key']  # 主键
            
            # 获取Oracle表的完整架构（字段名和类型）
            oracle_schema = self.oracle_reader.get_table_schema(table_name)
            logger.info(f"获取到Oracle表架构 / Retrieved Oracle table schema with {len(oracle_schema)} columns")
            
            # 获取列名
            columns = [col['column_name'] for col in oracle_schema]
            logger.info(f"表列 / Table columns: {', '.join(columns)}")
            
            # 构建增量同步参数
            last_sync_value = checkpoint.get('last_sync_value')
            
            if last_sync_value and not full_sync:
                logger.info(f"从上次同步值恢复 / Resuming from last sync value: {last_sync_value}")
            
            # 获取总记录数
            total_count = self.oracle_reader.get_total_count(
                table_name, 
                sync_column if (last_sync_value and not full_sync) else None,
                last_sync_value if not full_sync else None
            )
            logger.info(f"要同步的总记录数 / Total records to sync: {total_count}")
            
            if total_count == 0:
                logger.info("没有新记录需要同步 / No new records to sync")
                return
            
            # 初始化飞书表跟踪
            current_table_sequence = checkpoint.get('current_table_sequence', 1)
            current_table_id = checkpoint.get('current_table_id')
            # Only set current_table_id if it exists, otherwise let auto-creation happen
            # 只在存在时设置current_table_id，否则让自动创建发生
            if current_table_id:
                self.feishu_client.current_table_id = current_table_id
            elif self.feishu_client.base_table_id:
                self.feishu_client.current_table_id = self.feishu_client.base_table_id
            
            # 批量同步数据
            batch_size = self.config['sync']['read_batch_size']  # Oracle读取批次大小
            write_batch_size = self.config['sync']['write_batch_size']  # 飞书写入批次大小
            offset = 0
            total_synced = 0
            
            while offset < total_count:
                logger.info(f"处理批次 / Processing batch: offset={offset}, total={total_count}")
                
                # 从Oracle读取批次（使用参数化查询）
                records = self.oracle_reader.read_batch(
                    table_name=table_name,
                    columns=columns,
                    batch_size=batch_size,
                    offset=offset,
                    sync_column=sync_column if (last_sync_value and not full_sync) else None,
                    last_sync_value=last_sync_value if not full_sync else None,
                    order_by=sync_column  # 按同步列排序
                )
                
                if not records:
                    break
                
                # 写入飞书（分小批次，每批最多1000条）
                for i in range(0, len(records), write_batch_size):
                    write_batch = records[i:i + write_batch_size]
                    
                    # 写入记录并管理表切换（传递Oracle架构）
                    result = self.feishu_client.write_records_with_table_management(
                        write_batch,
                        current_table_sequence,
                        oracle_schema=oracle_schema
                    )
                    
                    # 更新跟踪信息
                    current_table_sequence = result['sequence']
                    total_synced += result['written']
                    
                    logger.info(f"写入 {result['written']} 条记录到表 {result['table_id']} / Written {result['written']} records to table {result['table_id']}")
                
                # 更新检查点
                last_record = records[-1]
                last_sync_value = last_record.get(sync_column)
                
                self.checkpoint_manager.update_sync_progress(
                    records_synced=len(records),
                    last_value=last_sync_value,
                    table_id=self.feishu_client.current_table_id,
                    table_sequence=current_table_sequence,
                    batch_offset=offset + len(records)
                )
                
                offset += len(records)
                
                # 防止除零错误
                if total_count > 0:
                    logger.info(f"进度 / Progress: {offset}/{total_count} ({offset*100//total_count}%)")
            
            # 汇总信息
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            logger.info("=" * 60)
            logger.info("同步成功完成 / Synchronization completed successfully")
            logger.info(f"总同步记录数 / Total records synced: {total_synced}")
            logger.info(f"耗时 / Duration: {duration:.2f} seconds")
            if total_synced > 0:
                logger.info(f"平均速度 / Average speed: {total_synced/duration:.2f} records/second")
            logger.info("=" * 60)
            
        except Exception as e:
            logger.error(f"同步失败 / Synchronization failed: {e}", exc_info=True)
            raise
        
        finally:
            # 清理资源
            if self.oracle_reader:
                self.oracle_reader.disconnect()


def main():
    """
    Main entry point
    主入口函数
    """
    import argparse
    
    parser = argparse.ArgumentParser(
        description='将数据从Oracle同步到飞书多维表格 / Synchronize data from Oracle to Feishu multi-dimensional tables'
    )
    parser.add_argument(
        '--config',
        default='config.yaml',
        help='配置文件路径 / Path to configuration file (default: config.yaml)'
    )
    parser.add_argument(
        '--full-sync',
        action='store_true',
        help='执行完整同步，忽略检查点 / Perform full sync, ignoring checkpoint'
    )
    parser.add_argument(
        '--reset-checkpoint',
        action='store_true',
        help='重置检查点并退出 / Reset checkpoint and exit'
    )
    
    args = parser.parse_args()
    
    # 如果是重置检查点操作
    if args.reset_checkpoint:
        checkpoint_file = 'sync_checkpoint.json'
        if Path('config.yaml').exists():
            with open('config.yaml', 'r') as f:
                config = yaml.safe_load(f)
                checkpoint_file = config['sync']['checkpoint_file']
        
        manager = CheckpointManager(checkpoint_file)
        manager.reset()
        logger.info(f"检查点已重置 / Checkpoint reset: {checkpoint_file}")
        return
    
    # 运行同步
    sync = OracleToFeishuSync(args.config)
    sync.run(full_sync=args.full_sync)


if __name__ == '__main__':
    main()
