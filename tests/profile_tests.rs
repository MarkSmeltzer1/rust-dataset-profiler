use dataset_profiler::readers::csv::{preview_csv, profile_csv};
use dataset_profiler::readers::json::{preview_json, profile_json};
use dataset_profiler::types::InferredType;
use std::fs;
use std::time::{SystemTime, UNIX_EPOCH};

fn temp_file(name: &str, content: &str) -> String {
    let unique_id = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock should be after Unix epoch")
        .as_nanos();
    let path = std::env::temp_dir().join(format!("{}_{}", unique_id, name));
    fs::write(&path, content).expect("test fixture should be written");
    path.to_string_lossy().to_string()
}

fn basic_csv_fixture() -> String {
    temp_file(
        "dprofile_basic.csv",
        "id,name,age\n1,Alice,25\n2,Bob,30\n3,,22\n",
    )
}

fn malformed_csv_fixture() -> String {
    temp_file(
        "dprofile_bad.csv",
        "id,name,age\n1,Alice,25\n2,Bob\n3,Charlie,40,extra\n4,,22\n",
    )
}

fn basic_json_fixture() -> String {
    temp_file(
        "dprofile_basic.json",
        r#"
[
  {"id": 1, "name": "Alice", "age": 25},
  {"id": 2, "name": "Bob", "age": 30},
  {"id": 3, "name": null, "age": 22}
]
"#,
    )
}

fn basic_jsonl_fixture() -> String {
    temp_file(
        "dprofile_basic.jsonl",
        r#"{"id":1,"name":"Alice","age":25}
{"id":2,"name":"Bob","age":30}
{"id":3,"name":null,"age":22}
"#,
    )
}

#[test]
fn test_csv_profile_basic() {
    let path = basic_csv_fixture();
    let profile = profile_csv(&path, b',').expect("CSV profiling should succeed");

    assert_eq!(profile.row_count, 3);
    assert_eq!(profile.column_count, 3);
    assert_eq!(profile.malformed_row_count, 0);
    assert_eq!(profile.columns.len(), 3);

    let id_col = profile.columns.iter().find(|c| c.name == "id").unwrap();
    assert_eq!(id_col.null_count, 0);
    assert_eq!(id_col.total_count, 3);
    assert!(matches!(id_col.inferred_type, InferredType::Integer));

    let name_col = profile.columns.iter().find(|c| c.name == "name").unwrap();
    assert_eq!(name_col.null_count, 1);
    assert_eq!(name_col.total_count, 3);
    assert!(matches!(name_col.inferred_type, InferredType::String));

    let age_col = profile.columns.iter().find(|c| c.name == "age").unwrap();
    assert_eq!(age_col.null_count, 0);
    assert_eq!(age_col.total_count, 3);
    assert!(matches!(age_col.inferred_type, InferredType::Integer));

    let _ = fs::remove_file(path);
}

#[test]
fn test_csv_profile_malformed_rows() {
    let path = malformed_csv_fixture();
    let profile = profile_csv(&path, b',').expect("Malformed CSV should still profile");

    assert_eq!(profile.row_count, 4);
    assert_eq!(profile.column_count, 3);
    assert_eq!(profile.malformed_row_count, 2);
    assert_eq!(profile.malformed_rows.len(), 2);

    assert_eq!(profile.malformed_rows[0].row_number, 2);
    assert_eq!(profile.malformed_rows[0].expected_fields, 3);
    assert_eq!(profile.malformed_rows[0].found_fields, 2);

    assert_eq!(profile.malformed_rows[1].row_number, 3);
    assert_eq!(profile.malformed_rows[1].expected_fields, 3);
    assert_eq!(profile.malformed_rows[1].found_fields, 4);

    let _ = fs::remove_file(path);
}

#[test]
fn test_csv_preview() {
    let path = basic_csv_fixture();
    let preview = preview_csv(&path, b',').expect("CSV preview should succeed");

    assert_eq!(preview.column_count, 3);
    assert_eq!(preview.headers, vec!["id", "name", "age"]);

    let _ = fs::remove_file(path);
}

#[test]
fn test_csv_headers_only_profiles_zero_rows() {
    let path = temp_file("dprofile_headers_only.csv", "id,name,age\n");
    let profile = profile_csv(&path, b',').expect("headers-only CSV should profile");

    assert_eq!(profile.row_count, 0);
    assert_eq!(profile.column_count, 3);
    assert_eq!(profile.malformed_row_count, 0);
    assert_eq!(profile.columns.len(), 3);

    let _ = fs::remove_file(path);
}

#[test]
fn test_empty_csv_returns_error() {
    let path = temp_file("dprofile_empty.csv", "");
    let result = profile_csv(&path, b',');

    assert!(result.is_err());

    let _ = fs::remove_file(path);
}

#[test]
fn test_json_profile_basic() {
    let path = basic_json_fixture();
    let profile = profile_json(&path).expect("JSON profiling should succeed");

    assert_eq!(profile.row_count, 3);
    assert_eq!(profile.column_count, 3);
    assert_eq!(profile.malformed_row_count, 0);
    assert_eq!(profile.columns.len(), 3);

    let id_col = profile.columns.iter().find(|c| c.name == "id").unwrap();
    assert_eq!(id_col.null_count, 0);
    assert_eq!(id_col.total_count, 3);
    assert!(matches!(id_col.inferred_type, InferredType::Integer));

    let name_col = profile.columns.iter().find(|c| c.name == "name").unwrap();
    assert_eq!(name_col.null_count, 1);
    assert_eq!(name_col.total_count, 3);
    assert!(matches!(name_col.inferred_type, InferredType::String));

    let age_col = profile.columns.iter().find(|c| c.name == "age").unwrap();
    assert_eq!(age_col.null_count, 0);
    assert_eq!(age_col.total_count, 3);
    assert!(matches!(age_col.inferred_type, InferredType::Integer));

    let _ = fs::remove_file(path);
}

#[test]
fn test_json_preview() {
    let path = basic_json_fixture();
    let preview = preview_json(&path).expect("JSON preview should succeed");

    assert_eq!(preview.column_count, 3);
    assert!(preview.keys.contains(&"id".to_string()));
    assert!(preview.keys.contains(&"name".to_string()));
    assert!(preview.keys.contains(&"age".to_string()));

    let _ = fs::remove_file(path);
}

#[test]
fn test_jsonl_profile_basic() {
    let path = basic_jsonl_fixture();
    let profile = profile_json(&path).expect("JSONL profiling should succeed");

    assert_eq!(profile.row_count, 3);
    assert_eq!(profile.column_count, 3);
    assert_eq!(profile.malformed_row_count, 0);

    let _ = fs::remove_file(path);
}

#[test]
fn test_json_missing_keys_count_as_nulls() {
    let path = temp_file(
        "dprofile_missing_keys.json",
        r#"
[
  {"id": 1, "name": "Alice"},
  {"id": 2, "age": 30}
]
"#,
    );

    let profile = profile_json(&path).expect("JSON with missing keys should profile");

    assert_eq!(profile.row_count, 2);
    assert_eq!(profile.column_count, 3);

    let name_col = profile.columns.iter().find(|c| c.name == "name").unwrap();
    assert_eq!(name_col.null_count, 1);
    assert_eq!(name_col.total_count, 2);

    let age_col = profile.columns.iter().find(|c| c.name == "age").unwrap();
    assert_eq!(age_col.null_count, 1);
    assert_eq!(age_col.total_count, 2);

    let _ = fs::remove_file(path);
}

#[test]
fn test_empty_json_preview_returns_error() {
    let path = temp_file("dprofile_empty.json", "");
    let result = preview_json(&path);

    assert!(result.is_err());

    let _ = fs::remove_file(path);
}

#[test]
fn test_invalid_json_profile_returns_error() {
    let path = temp_file("dprofile_invalid.json", "{ invalid json");
    let result = profile_json(&path);

    assert!(result.is_err());

    let _ = fs::remove_file(path);
}
