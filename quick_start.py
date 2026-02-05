#!/usr/bin/env python3
"""
Quick Start Example for Oracle to Feishu Sync
快速入门示例
"""

import yaml
from sync_oracle_to_feishu import OracleToFeishuSync

def example_basic_sync():
    """基础同步示例"""
    print("=" * 60)
    print("基础同步示例 - Basic Sync Example")
    print("=" * 60)
    
    # 方式1: 使用默认配置文件
    sync = OracleToFeishuSync('config.yaml')
    
    # 执行增量同步
    sync.run(full_sync=False)
    
    print("\n同步完成！Sync completed!")


def example_full_sync():
    """完整同步示例"""
    print("=" * 60)
    print("完整同步示例 - Full Sync Example")
    print("=" * 60)
    
    sync = OracleToFeishuSync('config.yaml')
    
    # 执行完整同步（忽略检查点）
    sync.run(full_sync=True)
    
    print("\n完整同步完成！Full sync completed!")


def example_custom_config():
    """自定义配置示例"""
    print("=" * 60)
    print("自定义配置示例 - Custom Config Example")
    print("=" * 60)
    
    # 可以编程方式创建配置
    config = {
        'oracle': {
            'host': 'localhost',
            'port': 1521,
            'service_name': 'ORCL',
            'username': 'your_user',
            'password': 'your_pass',
            'table_name': 'YOUR_TABLE',
            'primary_key': 'ID',
            'sync_column': 'UPDATED_AT'
        },
        'feishu': {
            'app_id': 'your_app_id',
            'app_secret': 'your_app_secret',
            'app_token': 'your_app_token',
            'base_table_id': 'your_table_id',
            'table_name_prefix': 'MyData',
            'max_rows_per_table': 20000
        },
        'sync': {
            'read_batch_size': 1000,
            'write_batch_size': 1000,
            'checkpoint_file': 'my_checkpoint.json',
            'max_requests_per_second': 50
        }
    }
    
    # 保存配置到文件
    with open('my_config.yaml', 'w', encoding='utf-8') as f:
        yaml.dump(config, f, allow_unicode=True)
    
    print("自定义配置已创建: my_config.yaml")
    print("Custom config created: my_config.yaml")
    
    # 使用自定义配置
    # sync = OracleToFeishuSync('my_config.yaml')
    # sync.run()


def example_check_progress():
    """查看同步进度示例"""
    print("=" * 60)
    print("查看同步进度 - Check Progress Example")
    print("=" * 60)
    
    from checkpoint_manager import CheckpointManager
    
    manager = CheckpointManager('sync_checkpoint.json')
    checkpoint = manager.get_checkpoint_data()
    
    print("\n当前同步状态 - Current Sync Status:")
    print(f"  总同步记录数 Total synced: {checkpoint['total_records_synced']}")
    print(f"  最后同步时间 Last sync time: {checkpoint['last_sync_time']}")
    print(f"  最后同步值 Last sync value: {checkpoint['last_sync_value']}")
    print(f"  当前表序号 Current table seq: {checkpoint['current_table_sequence']}")
    print(f"  当前表ID Current table ID: {checkpoint['current_table_id']}")
    
    if checkpoint['sync_history']:
        print("\n最近同步历史 - Recent History:")
        for entry in checkpoint['sync_history'][-5:]:
            print(f"  {entry['timestamp']}: {entry['records_synced']} records")


def example_reset_checkpoint():
    """重置检查点示例"""
    print("=" * 60)
    print("重置检查点示例 - Reset Checkpoint Example")
    print("=" * 60)
    
    from checkpoint_manager import CheckpointManager
    
    manager = CheckpointManager('sync_checkpoint.json')
    
    # 确认重置
    response = input("确认要重置检查点吗？这将导致下次同步从头开始。(y/N): ")
    
    if response.lower() == 'y':
        manager.reset()
        print("检查点已重置！Checkpoint reset!")
    else:
        print("操作已取消。Operation cancelled.")


if __name__ == '__main__':
    print("""
Oracle to Feishu 数据同步 - 快速入门
Oracle to Feishu Data Sync - Quick Start

选择要运行的示例 / Choose an example to run:
1. 基础增量同步 / Basic incremental sync
2. 完整同步 / Full sync
3. 创建自定义配置 / Create custom config
4. 查看同步进度 / Check progress
5. 重置检查点 / Reset checkpoint

请输入选项 (1-5) / Enter option (1-5):
    """)
    
    choice = input().strip()
    
    if choice == '1':
        example_basic_sync()
    elif choice == '2':
        example_full_sync()
    elif choice == '3':
        example_custom_config()
    elif choice == '4':
        example_check_progress()
    elif choice == '5':
        example_reset_checkpoint()
    else:
        print("无效选项 / Invalid option")
