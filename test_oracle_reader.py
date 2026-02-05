"""
Unit test for OracleDataReader timestamp conversion
OracleDataReader时间戳转换的单元测试
"""
import sys
from datetime import datetime, timezone

# Mock oracledb module if not available
try:
    import oracledb
except ImportError:
    print("Note: oracledb not installed, using mock for testing")
    import types
    oracledb = types.ModuleType('oracledb')
    oracledb.LOB = type('LOB', (), {})
    sys.modules['oracledb'] = oracledb

from oracle_reader import OracleDataReader

def test_prepare_sync_value_for_query():
    """Test the _prepare_sync_value_for_query method"""
    print("Testing OracleDataReader._prepare_sync_value_for_query")
    print("=" * 60)
    
    # Create a mock config (won't actually connect)
    config = {
        'host': 'localhost',
        'port': 1521,
        'service_name': 'ORCL',
        'username': 'test',
        'password': 'test',
        'convert_utc_to_utc8': True
    }
    
    reader = OracleDataReader(config)
    
    # Manually populate the schema cache (simulating what get_table_schema does)
    reader._table_schemas = {
        'TEST_TABLE': {
            'ID': 'NUMBER',
            'UPDATED_AT': 'DATE',
            'CREATED_AT': 'TIMESTAMP(6)',
            'NAME': 'VARCHAR2',
            'COUNT': 'NUMBER'
        }
    }
    
    print("\nTest cases:")
    
    # Test case 1: DATE column with millisecond timestamp
    timestamp_ms = 1770273378000
    result = reader._prepare_sync_value_for_query('TEST_TABLE', 'UPDATED_AT', timestamp_ms)
    expected_dt = datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)
    
    if isinstance(result, datetime) and result == expected_dt:
        print(f"✓ Test 1 passed: DATE column with timestamp {timestamp_ms}")
        print(f"  Converted to: {result}")
    else:
        print(f"✗ Test 1 failed: Expected datetime, got {type(result)}: {result}")
        return False
    
    # Test case 2: TIMESTAMP(6) column with millisecond timestamp
    result = reader._prepare_sync_value_for_query('TEST_TABLE', 'CREATED_AT', timestamp_ms)
    
    if isinstance(result, datetime) and result == expected_dt:
        print(f"✓ Test 2 passed: TIMESTAMP(6) column with timestamp {timestamp_ms}")
        print(f"  Converted to: {result}")
    else:
        print(f"✗ Test 2 failed: Expected datetime, got {type(result)}: {result}")
        return False
    
    # Test case 3: NUMBER column with numeric value (should NOT convert)
    numeric_value = 12345
    result = reader._prepare_sync_value_for_query('TEST_TABLE', 'ID', numeric_value)
    
    if result == numeric_value and isinstance(result, int):
        print(f"✓ Test 3 passed: NUMBER column with value {numeric_value}")
        print(f"  No conversion: {result}")
    else:
        print(f"✗ Test 3 failed: Expected {numeric_value}, got {result}")
        return False
    
    # Test case 4: VARCHAR2 column with string value (should NOT convert)
    string_value = "test_string"
    result = reader._prepare_sync_value_for_query('TEST_TABLE', 'NAME', string_value)
    
    if result == string_value:
        print(f"✓ Test 4 passed: VARCHAR2 column with string '{string_value}'")
        print(f"  No conversion: {result}")
    else:
        print(f"✗ Test 4 failed: Expected '{string_value}', got {result}")
        return False
    
    # Test case 5: DATE column with datetime object (should NOT convert)
    dt_value = datetime.now(tz=timezone.utc)
    result = reader._prepare_sync_value_for_query('TEST_TABLE', 'UPDATED_AT', dt_value)
    
    if result == dt_value and isinstance(result, datetime):
        print(f"✓ Test 5 passed: DATE column with datetime object")
        print(f"  No conversion needed: {result}")
    else:
        print(f"✗ Test 5 failed: Expected datetime object, got {result}")
        return False
    
    # Test case 6: None value (should remain None)
    result = reader._prepare_sync_value_for_query('TEST_TABLE', 'UPDATED_AT', None)
    
    if result is None:
        print(f"✓ Test 6 passed: None value remains None")
    else:
        print(f"✗ Test 6 failed: Expected None, got {result}")
        return False
    
    print("\n" + "=" * 60)
    print("✓ All OracleDataReader tests passed!")
    return True

if __name__ == '__main__':
    try:
        success = test_prepare_sync_value_for_query()
        sys.exit(0 if success else 1)
    except Exception as e:
        print(f"\n✗ Test failed with exception: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
