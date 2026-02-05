"""
Configuration Diagnostic Tool
配置诊断工具

This script checks your config.yaml for common issues and provides recommendations.
此脚本检查您的config.yaml中的常见问题并提供建议。
"""
import yaml
import sys
from pathlib import Path

def check_config(config_path='config.yaml'):
    """Check configuration file for issues"""
    print("=" * 60)
    print("Configuration Diagnostic Tool / 配置诊断工具")
    print("=" * 60)
    
    # Check if config file exists
    if not Path(config_path).exists():
        print(f"\n❌ Error: Configuration file '{config_path}' not found")
        print(f"   Please create it from config.yaml.example")
        print(f"   请从config.yaml.example创建配置文件")
        return False
    
    # Load config
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
    except Exception as e:
        print(f"\n❌ Error: Failed to load configuration file: {e}")
        return False
    
    print(f"\n✓ Configuration file loaded successfully")
    
    issues = []
    warnings = []
    
    # Check sync configuration
    if 'sync' in config:
        sync_config = config['sync']
        
        # Check read_batch_size
        read_batch_size = sync_config.get('read_batch_size', 1000)
        print(f"\n📊 Batch Size Configuration:")
        print(f"   read_batch_size: {read_batch_size}")
        
        if read_batch_size < 100:
            warnings.append(f"read_batch_size is very small ({read_batch_size}). Recommend 1000-10000 for better performance.")
            warnings.append(f"  → 建议使用1000-10000以获得更好的性能")
        elif read_batch_size < 1000:
            warnings.append(f"read_batch_size is {read_batch_size}. Consider increasing to 5000-10000 to reduce database queries.")
            warnings.append(f"  → 考虑增加到5000-10000以减少数据库查询次数")
        elif read_batch_size > 50000:
            warnings.append(f"read_batch_size is very large ({read_batch_size}). May cause memory issues.")
            warnings.append(f"  → 可能导致内存问题")
        elif read_batch_size >= 10000:
            print(f"   ✓ Good: Large batch size reduces database queries")
            print(f"     优秀：大批次减少数据库查询次数")
        
        # Check write_batch_size
        write_batch_size = sync_config.get('write_batch_size', 1000)
        print(f"   write_batch_size: {write_batch_size}")
        
        if write_batch_size > 1000:
            issues.append(f"write_batch_size ({write_batch_size}) exceeds Feishu API limit of 1000. Will be capped at 1000.")
        elif write_batch_size < 1000:
            warnings.append(f"write_batch_size is {write_batch_size}, which is less than the maximum allowed 1000.")
            warnings.append(f"  → Recommendation: Set write_batch_size to 1000 for optimal performance")
            warnings.append(f"  → 建议：将write_batch_size设置为1000以获得最佳性能")
            print(f"   ⚠️  Using {write_batch_size} instead of 1000 will result in:")
            print(f"       - More API calls (slower sync)")
            print(f"       - 使用{write_batch_size}而不是1000会导致：")
            print(f"         - 更多API调用（同步更慢）")
        
        # Check other sync settings
        checkpoint_file = sync_config.get('checkpoint_file', 'sync_checkpoint.json')
        max_requests = sync_config.get('max_requests_per_second', 50)
        
        print(f"   checkpoint_file: {checkpoint_file}")
        print(f"   max_requests_per_second: {max_requests}")
    else:
        issues.append("'sync' section missing from configuration")
    
    # Check Oracle configuration
    if 'oracle' in config:
        oracle_config = config['oracle']
        required_fields = ['host', 'port', 'service_name', 'username', 'password', 'table_name', 'sync_column', 'primary_key']
        missing_fields = [field for field in required_fields if field not in oracle_config]
        
        if missing_fields:
            issues.append(f"Oracle configuration missing fields: {', '.join(missing_fields)}")
        else:
            print(f"\n✓ Oracle configuration complete")
            print(f"   table_name: {oracle_config['table_name']}")
            print(f"   sync_column: {oracle_config['sync_column']}")
    else:
        issues.append("'oracle' section missing from configuration")
    
    # Check Feishu configuration
    if 'feishu' in config:
        feishu_config = config['feishu']
        required_fields = ['app_id', 'app_secret', 'app_token']
        missing_fields = [field for field in required_fields if field not in feishu_config]
        
        if missing_fields:
            issues.append(f"Feishu configuration missing fields: {', '.join(missing_fields)}")
        else:
            print(f"\n✓ Feishu configuration complete")
            
        max_rows = feishu_config.get('max_rows_per_table', 20000)
        if max_rows > 20000:
            warnings.append(f"max_rows_per_table ({max_rows}) exceeds Feishu limit of 20000")
    else:
        issues.append("'feishu' section missing from configuration")
    
    # Print summary
    print("\n" + "=" * 60)
    print("Diagnostic Summary / 诊断摘要")
    print("=" * 60)
    
    if issues:
        print(f"\n❌ Issues Found ({len(issues)}):")
        for i, issue in enumerate(issues, 1):
            print(f"   {i}. {issue}")
    
    if warnings:
        print(f"\n⚠️  Warnings ({len(warnings)}):")
        for i, warning in enumerate(warnings, 1):
            print(f"   {i}. {warning}")
    
    if not issues and not warnings:
        print("\n✅ Configuration looks good!")
        print("   配置看起来不错！")
        return True
    elif issues:
        print("\n❌ Please fix the issues above before running sync")
        print("   请在运行同步前修复上述问题")
        return False
    else:
        print("\n✓ Configuration is valid, but consider the warnings above")
        print("  配置有效，但请考虑上述警告")
        return True

if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='Check configuration file')
    parser.add_argument('--config', default='config.yaml', help='Path to configuration file')
    
    args = parser.parse_args()
    
    success = check_config(args.config)
    sys.exit(0 if success else 1)
