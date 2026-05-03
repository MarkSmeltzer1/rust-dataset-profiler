use dataset_profiler::readers::csv::{preview_csv, profile_csv};
use dataset_profiler::readers::json::{preview_json, profile_json};
use dataset_profiler::types::InferredType;
use std::fs;

fn temp_file(name: &str, content: &str) -> String {
    let path = std::env::temp_dir().join(name);
    fs::write(&path, content).expect("test fixture should be written");
    path.to_string_lossy().to_string()
}

#[test]
fn test_csv_profile_basic() {
    let profile = profile_csv("test.csv", b',').expect("CSV profiling should succeed");

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
}

#[test]
fn test_csv_profile_malformed_rows() {
    let profile = profile_csv("bad_test.csv", b',').expect("Malformed CSV should still profile");

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
}

#[test]
fn test_csv_preview() {
    let preview = preview_csv("test.csv", b',').expect("CSV preview should succeed");

    assert_eq!(preview.column_count, 3);
    assert_eq!(preview.headers, vec!["id", "name", "age"]);
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
    let profile = profile_json("test.json").expect("JSON profiling should succeed");

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
}

#[test]
fn test_json_preview() {
    let preview = preview_json("test.json").expect("JSON preview should succeed");

    assert_eq!(preview.column_count, 3);
    assert!(preview.keys.contains(&"id".to_string()));
    assert!(preview.keys.contains(&"name".to_string()));
    assert!(preview.keys.contains(&"age".to_string()));
}

#[test]
fn test_jsonl_profile_basic() {
    let profile = profile_json("test.jsonl").expect("JSONL profiling should succeed");

    assert_eq!(profile.row_count, 3);
    assert_eq!(profile.column_count, 3);
    assert_eq!(profile.malformed_row_count, 0);
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
