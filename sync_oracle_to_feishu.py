"""
Main Sync Program
Orchestrates data synchronization from Oracle to Feishu
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

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('sync.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)


class OracleToFeishuSync:
    """Main synchronization orchestrator"""
    
    def __init__(self, config_path: str = "config.yaml"):
        """
        Initialize sync orchestrator
        
        Args:
            config_path: Path to configuration file
        """
        self.config = self._load_config(config_path)
        self.checkpoint_manager = CheckpointManager(
            self.config['sync']['checkpoint_file']
        )
        self.oracle_reader = None
        self.feishu_client = None
    
    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """
        Load configuration from YAML file
        
        Args:
            config_path: Path to config file
            
        Returns:
            Configuration dictionary
        """
        config_file = Path(config_path)
        if not config_file.exists():
            raise FileNotFoundError(f"Config file not found: {config_path}")
        
        with open(config_file, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        
        logger.info(f"Loaded configuration from {config_path}")
        return config
    
    def run(self, full_sync: bool = False):
        """
        Run synchronization
        
        Args:
            full_sync: If True, ignore checkpoint and sync all data
        """
        logger.info("=" * 60)
        logger.info("Starting Oracle to Feishu synchronization")
        logger.info("=" * 60)
        
        start_time = datetime.now()
        
        try:
            # Initialize connections
            self.oracle_reader = OracleDataReader(self.config['oracle'])
            self.feishu_client = FeishuClient(self.config['feishu'])
            
            # Connect to Oracle
            self.oracle_reader.connect()
            
            # Get checkpoint data
            checkpoint = self.checkpoint_manager.get_checkpoint_data()
            
            if full_sync:
                logger.info("Full sync mode: ignoring checkpoint")
                checkpoint = self.checkpoint_manager._get_default_checkpoint()
                self.checkpoint_manager.reset()
            
            # Get table information
            table_name = self.config['oracle']['table_name']
            sync_column = self.config['oracle']['sync_column']
            primary_key = self.config['oracle']['primary_key']
            
            # Get columns
            columns = self.oracle_reader.get_table_columns(table_name)
            logger.info(f"Table columns: {', '.join(columns)}")
            
            # Build WHERE clause for incremental sync
            where_clause = ""
            last_sync_value = checkpoint.get('last_sync_value')
            
            if last_sync_value and not full_sync:
                logger.info(f"Resuming from last sync value: {last_sync_value}")
                if isinstance(last_sync_value, str):
                    where_clause = f"{sync_column} > '{last_sync_value}'"
                else:
                    where_clause = f"{sync_column} > {last_sync_value}"
            
            # Get total count
            total_count = self.oracle_reader.get_total_count(table_name, where_clause)
            logger.info(f"Total records to sync: {total_count}")
            
            if total_count == 0:
                logger.info("No new records to sync")
                return
            
            # Initialize Feishu table tracking
            current_table_sequence = checkpoint.get('current_table_sequence', 1)
            current_table_id = checkpoint.get('current_table_id') or self.feishu_client.base_table_id
            self.feishu_client.current_table_id = current_table_id
            
            # Sync data in batches
            batch_size = self.config['sync']['read_batch_size']
            write_batch_size = self.config['sync']['write_batch_size']
            offset = 0
            total_synced = 0
            
            while offset < total_count:
                logger.info(f"Processing batch: offset={offset}, total={total_count}")
                
                # Read batch from Oracle
                records = self.oracle_reader.read_batch(
                    table_name=table_name,
                    columns=columns,
                    batch_size=batch_size,
                    offset=offset,
                    where_clause=where_clause,
                    order_by=sync_column
                )
                
                if not records:
                    break
                
                # Write to Feishu in smaller batches (500 max)
                for i in range(0, len(records), write_batch_size):
                    write_batch = records[i:i + write_batch_size]
                    
                    result = self.feishu_client.write_records_with_table_management(
                        write_batch,
                        current_table_sequence
                    )
                    
                    # Update tracking
                    current_table_sequence = result['sequence']
                    total_synced += result['written']
                    
                    logger.info(f"Written {result['written']} records to table {result['table_id']}")
                
                # Update checkpoint
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
                
                logger.info(f"Progress: {offset}/{total_count} ({offset*100//total_count}%)")
            
            # Summary
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            logger.info("=" * 60)
            logger.info("Synchronization completed successfully")
            logger.info(f"Total records synced: {total_synced}")
            logger.info(f"Duration: {duration:.2f} seconds")
            logger.info(f"Average speed: {total_synced/duration:.2f} records/second")
            logger.info("=" * 60)
            
        except Exception as e:
            logger.error(f"Synchronization failed: {e}", exc_info=True)
            raise
        
        finally:
            # Cleanup
            if self.oracle_reader:
                self.oracle_reader.disconnect()


def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Synchronize data from Oracle to Feishu multi-dimensional tables'
    )
    parser.add_argument(
        '--config',
        default='config.yaml',
        help='Path to configuration file (default: config.yaml)'
    )
    parser.add_argument(
        '--full-sync',
        action='store_true',
        help='Perform full sync, ignoring checkpoint'
    )
    parser.add_argument(
        '--reset-checkpoint',
        action='store_true',
        help='Reset checkpoint and exit'
    )
    
    args = parser.parse_args()
    
    if args.reset_checkpoint:
        checkpoint_file = 'sync_checkpoint.json'
        if Path('config.yaml').exists():
            with open('config.yaml', 'r') as f:
                config = yaml.safe_load(f)
                checkpoint_file = config['sync']['checkpoint_file']
        
        manager = CheckpointManager(checkpoint_file)
        manager.reset()
        logger.info(f"Checkpoint reset: {checkpoint_file}")
        return
    
    sync = OracleToFeishuSync(args.config)
    sync.run(full_sync=args.full_sync)


if __name__ == '__main__':
    main()
