#[path = "../src/types.rs"]
mod types;

#[path = "../src/readers/csv.rs"]
mod csv_reader;

#[path = "../src/readers/json.rs"]
mod json_reader;

use csv_reader::{preview_csv, profile_csv};
use json_reader::{preview_json, profile_json};
use types::InferredType;

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