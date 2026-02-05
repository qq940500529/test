"""
Checkpoint Manager
Handles tracking sync progress for resumable transfers
"""
import json
import logging
from pathlib import Path
from typing import Dict, Any, Optional
from datetime import datetime

logger = logging.getLogger(__name__)


class CheckpointManager:
    """Manages checkpoint data for resumable sync"""
    
    def __init__(self, checkpoint_file: str):
        """
        Initialize checkpoint manager
        
        Args:
            checkpoint_file: Path to checkpoint file
        """
        self.checkpoint_file = Path(checkpoint_file)
        self.checkpoint_data = self._load()
    
    def _load(self) -> Dict[str, Any]:
        """
        Load checkpoint data from file
        
        Returns:
            Checkpoint data dictionary
        """
        if self.checkpoint_file.exists():
            try:
                with open(self.checkpoint_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                logger.info(f"Loaded checkpoint from {self.checkpoint_file}")
                return data
            except Exception as e:
                logger.warning(f"Failed to load checkpoint: {e}")
                return self._get_default_checkpoint()
        else:
            logger.info("No checkpoint file found, starting fresh")
            return self._get_default_checkpoint()
    
    def _get_default_checkpoint(self) -> Dict[str, Any]:
        """
        Get default checkpoint structure
        
        Returns:
            Default checkpoint data
        """
        return {
            "last_sync_time": None,
            "last_sync_value": None,  # Last value of sync_column processed
            "total_records_synced": 0,
            "current_table_sequence": 1,
            "current_table_id": None,
            "last_batch_offset": 0,
            "sync_history": []
        }
    
    def save(self):
        """Save checkpoint data to file"""
        try:
            # Update last sync time
            self.checkpoint_data["last_sync_time"] = datetime.now().isoformat()
            
            with open(self.checkpoint_file, 'w', encoding='utf-8') as f:
                json.dump(self.checkpoint_data, f, indent=2, ensure_ascii=False)
            logger.info(f"Saved checkpoint to {self.checkpoint_file}")
        except Exception as e:
            logger.error(f"Failed to save checkpoint: {e}")
    
    def get_last_sync_value(self) -> Optional[Any]:
        """
        Get last synced value for incremental sync
        
        Returns:
            Last sync value or None
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
        
        Args:
            records_synced: Number of records synced in this batch
            last_value: Last value of sync column processed
            table_id: Current Feishu table ID
            table_sequence: Current table sequence number
            batch_offset: Current batch offset
        """
        self.checkpoint_data["total_records_synced"] += records_synced
        self.checkpoint_data["last_sync_value"] = last_value
        self.checkpoint_data["current_table_id"] = table_id
        self.checkpoint_data["current_table_sequence"] = table_sequence
        self.checkpoint_data["last_batch_offset"] = batch_offset
        
        # Add to history
        history_entry = {
            "timestamp": datetime.now().isoformat(),
            "records_synced": records_synced,
            "last_value": last_value,
            "table_id": table_id
        }
        
        self.checkpoint_data["sync_history"].append(history_entry)
        
        # Keep only last 100 history entries
        if len(self.checkpoint_data["sync_history"]) > 100:
            self.checkpoint_data["sync_history"] = self.checkpoint_data["sync_history"][-100:]
        
        self.save()
    
    def get_checkpoint_data(self) -> Dict[str, Any]:
        """
        Get full checkpoint data
        
        Returns:
            Checkpoint data dictionary
        """
        return self.checkpoint_data.copy()
    
    def reset(self):
        """Reset checkpoint to start fresh"""
        self.checkpoint_data = self._get_default_checkpoint()
        self.save()
        logger.info("Checkpoint reset")
