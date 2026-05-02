use std::collections::{BTreeSet, HashMap};
use std::error::Error;
use std::fs;
use std::fs::File;
use std::io::{BufRead, BufReader};

use serde_json::Value;
use tracing::info;

use crate::types::{ColumnProfile, InferredType, JsonPreview, JsonProfile};

const PROGRESS_INTERVAL_ROWS: usize = 100_000;

pub fn preview_json(file_path: &str) -> Result<JsonPreview, Box<dyn Error>> {
    info!("Opening JSON file for dry run: {}", file_path);

    let content = fs::read_to_string(file_path)?;
    let keys = extract_json_keys(&content)?;
    let column_count = keys.len();

    info!("Dry run found {} JSON keys", column_count);

    Ok(JsonPreview {
        file_path: file_path.to_string(),
        column_count,
        keys,
    })
}

pub fn profile_json(file_path: &str) -> Result<JsonProfile, Box<dyn Error>> {
    info!("Opening JSON file for full profiling: {}", file_path);

    let content = fs::read_to_string(file_path)?;
    let trimmed = content.trim();

    let records = if trimmed.starts_with('[') {
        parse_json_array(trimmed)?
    } else {
        parse_ndjson(file_path)?
    };

    let mut all_keys: BTreeSet<String> = BTreeSet::new();
    for record in &records {
        if let Value::Object(map) = record {
            for key in map.keys() {
                all_keys.insert(key.clone());
            }
        }
    }

    let keys: Vec<String> = all_keys.into_iter().collect();
    let column_count = keys.len();

    let mut columns: Vec<ColumnProfile> = keys
        .iter()
        .map(|key| ColumnProfile {
            name: key.clone(),
            null_count: 0,
            total_count: 0,
            inferred_type: InferredType::Unknown,
            numeric_min: None,
            numeric_max: None,
            numeric_sum: 0.0,
            numeric_count: 0,
            min_length: None,
            max_length: None,
            total_length: 0,
            non_null_count: 0,
        })
        .collect();

    let index_map: HashMap<String, usize> = keys
        .iter()
        .enumerate()
        .map(|(i, key)| (key.clone(), i))
        .collect();

    let mut row_count = 0usize;
    let mut malformed_row_count = 0usize;
    let mut malformed_rows: Vec<usize> = Vec::new();
    let mut total_row_width = 0usize;

    for (row_idx, record) in records.iter().enumerate() {
        row_count += 1;

        if row_count % PROGRESS_INTERVAL_ROWS == 0 {
            info!("JSON progress: {} rows processed", row_count);
        }

        let obj = match record {
            Value::Object(map) => map,
            _ => {
                malformed_row_count += 1;
                malformed_rows.push(row_idx + 1);
                continue;
            }
        };

        let mut row_width = 0usize;

        for key in &keys {
            let col_idx = index_map[key];
            columns[col_idx].total_count += 1;

            let value = obj.get(key).unwrap_or(&Value::Null);

            if value.is_null() {
                columns[col_idx].null_count += 1;
                continue;
            }

            columns[col_idx].non_null_count += 1;

            let inferred = infer_json_value_type(value);
            columns[col_idx].inferred_type =
                merge_inferred_types(&columns[col_idx].inferred_type, &inferred);

            match value {
                Value::Number(num) => {
                    if let Some(v) = num.as_f64() {
                        columns[col_idx].numeric_min = Some(match columns[col_idx].numeric_min {
                            Some(current) => current.min(v),
                            None => v,
                        });

                        columns[col_idx].numeric_max = Some(match columns[col_idx].numeric_max {
                            Some(current) => current.max(v),
                            None => v,
                        });

                        columns[col_idx].numeric_sum += v;
                        columns[col_idx].numeric_count += 1;

                        let len = num.to_string().len();
                        row_width += len;
                        update_length_stats(&mut columns[col_idx], len);
                    }
                }
                Value::String(s) => {
                    let len = s.len();
                    row_width += len;
                    update_length_stats(&mut columns[col_idx], len);
                }
                Value::Bool(b) => {
                    let len = b.to_string().len();
                    row_width += len;
                    update_length_stats(&mut columns[col_idx], len);
                }
                other => {
                    let serialized = other.to_string();
                    let len = serialized.len();
                    row_width += len;
                    update_length_stats(&mut columns[col_idx], len);
                }
            }
        }

        total_row_width += row_width;
    }

    info!("Processed {} JSON records", row_count);
    info!("Found {} malformed JSON records", malformed_row_count);

    Ok(JsonProfile {
        file_path: file_path.to_string(),
        row_count,
        column_count,
        malformed_row_count,
        malformed_rows,
        columns,
        total_row_width,
    })
}

fn parse_json_array(content: &str) -> Result<Vec<Value>, Box<dyn Error>> {
    let parsed: Value = serde_json::from_str(content)?;
    match parsed {
        Value::Array(arr) => Ok(arr),
        _ => Err("Expected top-level JSON array".into()),
    }
}

fn parse_ndjson(file_path: &str) -> Result<Vec<Value>, Box<dyn Error>> {
    let file = File::open(file_path)?;
    let reader = BufReader::new(file);

    let mut values = Vec::new();

    for line in reader.lines() {
        let line = line?;
        let trimmed = line.trim();

        if trimmed.is_empty() {
            continue;
        }

        let value: Value = serde_json::from_str(trimmed)?;
        values.push(value);
    }

    Ok(values)
}

fn extract_json_keys(content: &str) -> Result<Vec<String>, Box<dyn Error>> {
    let trimmed = content.trim();

    let first_value = if trimmed.starts_with('[') {
        let parsed: Value = serde_json::from_str(trimmed)?;
        match parsed {
            Value::Array(arr) => arr.into_iter().next().ok_or("Empty JSON array")?,
            _ => return Err("Expected top-level JSON array".into()),
        }
    } else {
        let first_nonempty = trimmed
            .lines()
            .find(|line| !line.trim().is_empty())
            .ok_or("Empty JSON file")?;
        serde_json::from_str(first_nonempty)?
    };

    match first_value {
        Value::Object(map) => Ok(map.keys().cloned().collect()),
        _ => Err("Expected JSON object records".into()),
    }
}

fn infer_json_value_type(value: &Value) -> InferredType {
    match value {
        Value::Null => InferredType::Unknown,
        Value::Bool(_) => InferredType::Boolean,
        Value::Number(num) => {
            if num.is_i64() || num.is_u64() {
                InferredType::Integer
            } else {
                InferredType::Float
            }
        }
        Value::String(_) => InferredType::String,
        _ => InferredType::Mixed,
    }
}

fn merge_inferred_types(current: &InferredType, new_type: &InferredType) -> InferredType {
    use InferredType::*;

    match (current, new_type) {
        (Unknown, t) => t.clone(),
        (Integer, Integer) => Integer,
        (Float, Float) => Float,
        (Boolean, Boolean) => Boolean,
        (String, String) => String,
        (Integer, Float) | (Float, Integer) => Float,
        (Mixed, _) | (_, Mixed) => Mixed,
        (current_type, new_type)
            if std::mem::discriminant(current_type) == std::mem::discriminant(new_type) =>
        {
            current_type.clone()
        }
        _ => Mixed,
    }
}

fn update_length_stats(column: &mut ColumnProfile, len: usize) {
    column.total_length += len;

    column.min_length = Some(match column.min_length {
        Some(current) => current.min(len),
        None => len,
    });

    column.max_length = Some(match column.max_length {
        Some(current) => current.max(len),
        None => len,
    });
}
