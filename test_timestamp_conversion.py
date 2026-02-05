"""
Test script for timestamp conversion logic
测试时间戳转换逻辑的脚本
"""
import sys
from datetime import datetime, timezone

# Test the timestamp conversion logic
def test_timestamp_conversion():
    """Test converting millisecond timestamp to datetime"""
    # Test with the timestamp from the error log: 1770273378000
    timestamp_ms = 1770273378000
    timestamp_sec = timestamp_ms / 1000
    dt = datetime.fromtimestamp(timestamp_sec, tz=timezone.utc)
    
    print(f"Original timestamp (ms): {timestamp_ms}")
    print(f"Converted to datetime: {dt}")
    print(f"ISO format: {dt.isoformat()}")
    
    # Verify round-trip conversion
    back_to_ms = int(dt.timestamp() * 1000)
    print(f"Back to timestamp (ms): {back_to_ms}")
    
    if timestamp_ms == back_to_ms:
        print("✓ Round-trip conversion successful!")
        return True
    else:
        print("✗ Round-trip conversion failed!")
        return False

def test_column_type_detection():
    """Test column type detection logic"""
    # Simulate table schema
    table_schemas = {
        'TEST_TABLE': {
            'ID': 'NUMBER',
            'UPDATED_AT': 'DATE',
            'CREATED_AT': 'TIMESTAMP(6)',
            'NAME': 'VARCHAR2'
        }
    }
    
    print("\nTable schema:")
    for col_name, col_type in table_schemas['TEST_TABLE'].items():
        print(f"  {col_name}: {col_type}")
    
    # Test type detection
    test_cases = [
        ('UPDATED_AT', 'DATE', True),
        ('CREATED_AT', 'TIMESTAMP(6)', True),
        ('ID', 'NUMBER', False),
        ('NAME', 'VARCHAR2', False)
    ]
    
    print("\nColumn type checks:")
    all_passed = True
    for col_name, expected_type, should_convert in test_cases:
        actual_type = table_schemas['TEST_TABLE'][col_name]
        is_date_type = actual_type in ('DATE', 'TIMESTAMP', 'TIMESTAMP(6)')
        
        result = "✓" if is_date_type == should_convert else "✗"
        print(f"  {result} {col_name}: {actual_type} (should convert: {should_convert}, would convert: {is_date_type})")
        
        if is_date_type != should_convert:
            all_passed = False
    
    return all_passed

def test_prepare_sync_value():
    """Test the sync value preparation logic"""
    print("\nSync value preparation tests:")
    
    # Test case 1: DATE column with numeric timestamp
    timestamp_ms = 1770273378000
    column_type = 'DATE'
    
    if column_type in ('DATE', 'TIMESTAMP', 'TIMESTAMP(6)') and isinstance(timestamp_ms, (int, float)):
        timestamp_sec = timestamp_ms / 1000
        converted = datetime.fromtimestamp(timestamp_sec, tz=timezone.utc)
        print(f"  ✓ DATE column with timestamp {timestamp_ms} → {converted}")
    else:
        print(f"  ✗ Failed to convert DATE column with timestamp")
        return False
    
    # Test case 2: NUMBER column with numeric value (should not convert)
    numeric_value = 12345
    column_type = 'NUMBER'
    
    if column_type in ('DATE', 'TIMESTAMP', 'TIMESTAMP(6)') and isinstance(numeric_value, (int, float)):
        print(f"  ✗ NUMBER column incorrectly marked for conversion")
        return False
    else:
        print(f"  ✓ NUMBER column with value {numeric_value} → {numeric_value} (no conversion)")
    
    # Test case 3: DATE column with datetime value (should not convert)
    datetime_value = datetime.now(tz=timezone.utc)
    column_type = 'DATE'
    
    if column_type in ('DATE', 'TIMESTAMP', 'TIMESTAMP(6)') and isinstance(datetime_value, (int, float)):
        print(f"  ✗ DATE column with datetime incorrectly marked for conversion")
        return False
    else:
        print(f"  ✓ DATE column with datetime {datetime_value} → {datetime_value} (no conversion needed)")
    
    return True

if __name__ == '__main__':
    print("=" * 60)
    print("Testing Timestamp Conversion Logic")
    print("=" * 60)
    
    test1_passed = test_timestamp_conversion()
    test2_passed = test_column_type_detection()
    test3_passed = test_prepare_sync_value()
    
    print("\n" + "=" * 60)
    if test1_passed and test2_passed and test3_passed:
        print("✓ All tests passed!")
        sys.exit(0)
    else:
        print("✗ Some tests failed!")
        sys.exit(1)
