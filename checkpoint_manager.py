"""
Checkpoint Manager
检查点管理器

Handles tracking sync progress for resumable transfers
处理同步进度跟踪，实现断点续传功能
"""
import json
import logging
from pathlib import Path
from typing import Dict, Any, Optional
from datetime import datetime

logger = logging.getLogger(__name__)


class CheckpointManager:
    """
    Manages checkpoint data for resumable sync
    管理检查点数据，实现可恢复的同步
    """
    
    def __init__(self, checkpoint_file: str):
        """
        Initialize checkpoint manager
        初始化检查点管理器
        
        Args:
            checkpoint_file: Path to checkpoint file / 检查点文件路径
        """
        self.checkpoint_file = Path(checkpoint_file)
        self.checkpoint_data = self._load()  # 加载检查点数据
    
    def _load(self) -> Dict[str, Any]:
        """
        Load checkpoint data from file
        从文件加载检查点数据
        
        Returns:
            Checkpoint data dictionary / 检查点数据字典
        """
        if self.checkpoint_file.exists():
            try:
                with open(self.checkpoint_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                logger.info(f"从 {self.checkpoint_file} 加载检查点 / Loaded checkpoint from {self.checkpoint_file}")
                return data
            except Exception as e:
                logger.warning(f"加载检查点失败 / Failed to load checkpoint: {e}")
                return self._get_default_checkpoint()
        else:
            logger.info("未找到检查点文件，从头开始 / No checkpoint file found, starting fresh")
            return self._get_default_checkpoint()
    
    def _get_default_checkpoint(self) -> Dict[str, Any]:
        """
        Get default checkpoint structure
        获取默认检查点结构
        
        Returns:
            Default checkpoint data / 默认检查点数据
        """
        return {
            "last_sync_time": None,  # 最后同步时间
            "last_sync_value": None,  # 最后处理的同步列值（用于增量同步）
            "total_records_synced": 0,  # 已同步的总记录数
            "current_table_sequence": 1,  # 当前表序号
            "current_table_id": None,  # 当前飞书表ID
            "last_batch_offset": 0,  # 最后批次的偏移量
            "sync_history": []  # 同步历史记录
        }
    
    def save(self):
        """
        Save checkpoint data to file
        保存检查点数据到文件
        """
        try:
            # 更新最后同步时间
            self.checkpoint_data["last_sync_time"] = datetime.now().isoformat()
            
            # 写入JSON文件
            with open(self.checkpoint_file, 'w', encoding='utf-8') as f:
                json.dump(self.checkpoint_data, f, indent=2, ensure_ascii=False)
            logger.info(f"检查点已保存到 {self.checkpoint_file} / Saved checkpoint to {self.checkpoint_file}")
        except Exception as e:
            logger.error(f"保存检查点失败 / Failed to save checkpoint: {e}")
    
    def get_last_sync_value(self) -> Optional[Any]:
        """
        Get last synced value for incremental sync
        获取最后同步的值，用于增量同步
        
        Returns:
            Last sync value or None / 最后同步值或None
        """
        return self.checkpoint_data.get("last_sync_value")
    
    def update_sync_progress(
        self,
        records_synced: int,
        last_value: Any,
        table_id: str,
        table_sequence: int,
        batch_offset: int = 0
    ):
        """
        Update sync progress
        更新同步进度
        
        Args:
            records_synced: Number of records synced in this batch / 本批次同步的记录数
            last_value: Last value of sync column processed / 最后处理的同步列值
            table_id: Current Feishu table ID / 当前飞书表ID
            table_sequence: Current table sequence number / 当前表序号
            batch_offset: Current batch offset / 当前批次偏移量
        """
        # 累加总同步记录数
        self.checkpoint_data["total_records_synced"] += records_synced
        # 更新最后同步值
        self.checkpoint_data["last_sync_value"] = last_value
        # 更新当前表信息
        self.checkpoint_data["current_table_id"] = table_id
        self.checkpoint_data["current_table_sequence"] = table_sequence
        self.checkpoint_data["last_batch_offset"] = batch_offset
        
        # 添加到历史记录
        history_entry = {
            "timestamp": datetime.now().isoformat(),
            "records_synced": records_synced,
            "last_value": last_value,
            "table_id": table_id
        }
        
        self.checkpoint_data["sync_history"].append(history_entry)
        
        # 只保留最近100条历史记录
        if len(self.checkpoint_data["sync_history"]) > 100:
            self.checkpoint_data["sync_history"] = self.checkpoint_data["sync_history"][-100:]
        
        # 保存到文件
        self.save()
    
    def get_checkpoint_data(self) -> Dict[str, Any]:
        """
        Get full checkpoint data
        获取完整的检查点数据
        
        Returns:
            Checkpoint data dictionary / 检查点数据字典
        """
        return self.checkpoint_data.copy()
    
    def get_fresh_checkpoint(self) -> Dict[str, Any]:
        """
        Get a fresh checkpoint (for full sync)
        获取一个新的检查点（用于完整同步）
        
        Returns:
            Fresh checkpoint data / 新的检查点数据
        """
        return self._get_default_checkpoint()
    
    def reset(self):
        """
        Reset checkpoint to start fresh
        重置检查点，从头开始
        """
        self.checkpoint_data = self._get_default_checkpoint()
        self.save()
        logger.info("检查点已重置 / Checkpoint reset")
