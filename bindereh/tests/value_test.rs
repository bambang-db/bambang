use shared_types::Value;

#[test]
fn test_null_serialization() {
    let value = Value::Null;
    let bytes = value.to_bytes();
    let mut offset = 0;
    let deserialized = Value::from_bytes(&bytes, &mut offset).unwrap();
    assert_eq!(value, deserialized);
    assert_eq!(offset, bytes.len());
}

#[test]
fn test_integer_serialization() {
    let value = Value::Integer(42);
    let bytes = value.to_bytes();
    let mut offset = 0;
    let deserialized = Value::from_bytes(&bytes, &mut offset).unwrap();
    assert_eq!(value, deserialized);
    assert_eq!(offset, bytes.len());

    // Test negative number
    let value = Value::Integer(-12345);
    let bytes = value.to_bytes();
    let mut offset = 0;
    let deserialized = Value::from_bytes(&bytes, &mut offset).unwrap();
    assert_eq!(value, deserialized);
}

#[test]
fn test_string_serialization() {
    let value = Value::String("Hello, World!".to_string());
    let bytes = value.to_bytes();
    let mut offset = 0;
    let deserialized = Value::from_bytes(&bytes, &mut offset).unwrap();
    assert_eq!(value, deserialized);
    assert_eq!(offset, bytes.len());

    // Test empty string
    let value = Value::String("".to_string());
    let bytes = value.to_bytes();
    let mut offset = 0;
    let deserialized = Value::from_bytes(&bytes, &mut offset).unwrap();
    assert_eq!(value, deserialized);
}

#[test]
fn test_float_serialization() {
    let value = Value::Float(3.14159);
    let bytes = value.to_bytes();
    let mut offset = 0;
    let deserialized = Value::from_bytes(&bytes, &mut offset).unwrap();
    assert_eq!(value, deserialized);
    assert_eq!(offset, bytes.len());

    // Test special values
    let value = Value::Float(f64::NAN);
    let bytes = value.to_bytes();
    let mut offset = 0;
    let deserialized = Value::from_bytes(&bytes, &mut offset).unwrap();
    if let Value::Float(f) = deserialized {
        assert!(f.is_nan());
    } else {
        panic!("Expected float");
    }
}

#[test]
fn test_boolean_serialization() {
    let value = Value::Boolean(true);
    let bytes = value.to_bytes();
    let mut offset = 0;
    let deserialized = Value::from_bytes(&bytes, &mut offset).unwrap();
    assert_eq!(value, deserialized);

    let value = Value::Boolean(false);
    let bytes = value.to_bytes();
    let mut offset = 0;
    let deserialized = Value::from_bytes(&bytes, &mut offset).unwrap();
    assert_eq!(value, deserialized);
}

#[test]
fn test_smallint_serialization() {
    let value = Value::SmallInt(32767);
    let bytes = value.to_bytes();
    let mut offset = 0;
    let deserialized = Value::from_bytes(&bytes, &mut offset).unwrap();
    assert_eq!(value, deserialized);

    let value = Value::SmallInt(-32768);
    let bytes = value.to_bytes();
    let mut offset = 0;
    let deserialized = Value::from_bytes(&bytes, &mut offset).unwrap();
    assert_eq!(value, deserialized);
}

#[test]
fn test_bigint_serialization() {
    let value = Value::BigInt(170141183460469231731687303715884105727);
    let bytes = value.to_bytes();
    let mut offset = 0;
    let deserialized = Value::from_bytes(&bytes, &mut offset).unwrap();
    assert_eq!(value, deserialized);

    let value = Value::BigInt(-170141183460469231731687303715884105728);
    let bytes = value.to_bytes();
    let mut offset = 0;
    let deserialized = Value::from_bytes(&bytes, &mut offset).unwrap();
    assert_eq!(value, deserialized);
}

#[test]
fn test_decimal_serialization() {
    let value = Value::Decimal("123.456789".to_string());
    let bytes = value.to_bytes();
    let mut offset = 0;
    let deserialized = Value::from_bytes(&bytes, &mut offset).unwrap();
    assert_eq!(value, deserialized);
}

#[test]
fn test_binary_serialization() {
    let value = Value::Binary(vec![0x01, 0x02, 0x03, 0xFF]);
    let bytes = value.to_bytes();
    let mut offset = 0;
    let deserialized = Value::from_bytes(&bytes, &mut offset).unwrap();
    assert_eq!(value, deserialized);

    // Test empty binary
    let value = Value::Binary(vec![]);
    let bytes = value.to_bytes();
    let mut offset = 0;
    let deserialized = Value::from_bytes(&bytes, &mut offset).unwrap();
    assert_eq!(value, deserialized);
}

#[test]
fn test_date_serialization() {
    let value = Value::Date(18628); // 2021-01-01
    let bytes = value.to_bytes();
    let mut offset = 0;
    let deserialized = Value::from_bytes(&bytes, &mut offset).unwrap();
    assert_eq!(value, deserialized);
}

#[test]
fn test_time_serialization() {
    let value = Value::Time(43200000); // 12:00:00
    let bytes = value.to_bytes();
    let mut offset = 0;
    let deserialized = Value::from_bytes(&bytes, &mut offset).unwrap();
    assert_eq!(value, deserialized);
}

#[test]
fn test_timestamp_serialization() {
    let value = Value::Timestamp(1609459200000); // 2021-01-01 00:00:00 UTC
    let bytes = value.to_bytes();
    let mut offset = 0;
    let deserialized = Value::from_bytes(&bytes, &mut offset).unwrap();
    assert_eq!(value, deserialized);
}

#[test]
fn test_datetime_serialization() {
    let value = Value::DateTime(1609459200000); // 2021-01-01 00:00:00 UTC
    let bytes = value.to_bytes();
    let mut offset = 0;
    let deserialized = Value::from_bytes(&bytes, &mut offset).unwrap();
    assert_eq!(value, deserialized);
}

#[test]
fn test_json_serialization() {
    let value = Value::Json(r#"{"name": "John", "age": 30}"#.to_string());
    let bytes = value.to_bytes();
    let mut offset = 0;
    let deserialized = Value::from_bytes(&bytes, &mut offset).unwrap();
    assert_eq!(value, deserialized);
}

#[test]
fn test_uuid_serialization() {
    let uuid = [
        0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0, 0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde,
        0xf0,
    ];
    let value = Value::Uuid(uuid);
    let bytes = value.to_bytes();
    let mut offset = 0;
    let deserialized = Value::from_bytes(&bytes, &mut offset).unwrap();
    assert_eq!(value, deserialized);
}

#[test]
fn test_text_serialization() {
    let value = Value::Text("This is a large text field".to_string());
    let bytes = value.to_bytes();
    let mut offset = 0;
    let deserialized = Value::from_bytes(&bytes, &mut offset).unwrap();
    assert_eq!(value, deserialized);
}

#[test]
fn test_char_serialization() {
    let value = Value::Char('A');
    let bytes = value.to_bytes();
    let mut offset = 0;
    let deserialized = Value::from_bytes(&bytes, &mut offset).unwrap();
    assert_eq!(value, deserialized);

    // Test Unicode character
    let value = Value::Char('🦀');
    let bytes = value.to_bytes();
    let mut offset = 0;
    let deserialized = Value::from_bytes(&bytes, &mut offset).unwrap();
    assert_eq!(value, deserialized);
}

#[test]
fn test_tinyint_serialization() {
    let value = Value::TinyInt(127);
    let bytes = value.to_bytes();
    let mut offset = 0;
    let deserialized = Value::from_bytes(&bytes, &mut offset).unwrap();
    assert_eq!(value, deserialized);

    let value = Value::TinyInt(-128);
    let bytes = value.to_bytes();
    let mut offset = 0;
    let deserialized = Value::from_bytes(&bytes, &mut offset).unwrap();
    assert_eq!(value, deserialized);
}

#[test]
fn test_serialized_size() {
    assert_eq!(Value::Null.serialized_size(), 1);
    assert_eq!(Value::Integer(42).serialized_size(), 9);
    assert_eq!(Value::String("hello".to_string()).serialized_size(), 10);
    assert_eq!(Value::Float(3.14).serialized_size(), 9);
    assert_eq!(Value::Boolean(true).serialized_size(), 2);
    assert_eq!(Value::SmallInt(100).serialized_size(), 3);
    assert_eq!(Value::BigInt(123456789).serialized_size(), 17);
    assert_eq!(Value::Binary(vec![1, 2, 3]).serialized_size(), 8);
    assert_eq!(Value::Char('A').serialized_size(), 4);
    assert_eq!(Value::Char('🦀').serialized_size(), 7); // 4-byte Unicode char
    assert_eq!(Value::TinyInt(42).serialized_size(), 2);
}

#[test]
fn test_type_name() {
    assert_eq!(Value::Null.type_name(), "null");
    assert_eq!(Value::Integer(42).type_name(), "integer");
    assert_eq!(Value::String("test".to_string()).type_name(), "string");
    assert_eq!(Value::Float(3.14).type_name(), "float");
    assert_eq!(Value::Boolean(true).type_name(), "boolean");
}

#[test]
fn test_is_null() {
    assert!(Value::Null.is_null());
    assert!(!Value::Integer(42).is_null());
    assert!(!Value::String("test".to_string()).is_null());
}

#[test]
fn test_corrupted_data_errors() {
    // Test truncated data
    let bytes = vec![1]; // Integer type but no data
    let mut offset = 0;
    let result = Value::from_bytes(&bytes, &mut offset);
    assert!(result.is_err());

    // Test invalid type marker
    let bytes = vec![99]; // Invalid type marker
    let mut offset = 0;
    let result = Value::from_bytes(&bytes, &mut offset);
    assert!(result.is_err());

    // Test invalid string length
    let bytes = vec![2, 255, 255, 255, 255]; // String type with huge length
    let mut offset = 0;
    let result = Value::from_bytes(&bytes, &mut offset);
    assert!(result.is_err());
}

#[test]
fn test_multiple_values_serialization() {
    let values = vec![
        Value::Integer(42),
        Value::String("hello".to_string()),
        Value::Boolean(true),
        Value::Null,
        Value::Float(3.14),
    ];

    let mut all_bytes = Vec::new();
    for value in &values {
        all_bytes.extend(value.to_bytes());
    }

    let mut offset = 0;
    let mut deserialized = Vec::new();
    for _ in 0..values.len() {
        deserialized.push(Value::from_bytes(&all_bytes, &mut offset).unwrap());
    }

    assert_eq!(values, deserialized);
    assert_eq!(offset, all_bytes.len());
}
