use std::error::Error;
use std::fs::File;

use csv::StringRecord;

use crate::types::{ColumnProfile, CsvProfile, InferredType};

pub fn profile_csv(file_path: &str, delimiter: u8) -> Result<CsvProfile, Box<dyn Error>> {
    let file = File::open(file_path)?;
    let mut reader = csv::ReaderBuilder::new()
        .delimiter(delimiter)
        .from_reader(file);

    let headers = reader
        .headers()?
        .iter()
        .map(|h| h.to_string())
        .collect::<Vec<String>>();

    let column_count = headers.len();
    let mut row_count = 0usize;

    let mut columns: Vec<ColumnProfile> = headers
        .iter()
        .map(|header| ColumnProfile {
            name: header.clone(),
            null_count: 0,
            total_count: 0,
            inferred_type: InferredType::Unknown,
        })
        .collect();

    for result in reader.records() {
        let record: StringRecord = result?;
        row_count += 1;

        for (i, field) in record.iter().enumerate() {
            columns[i].total_count += 1;

            let trimmed = field.trim();

            if trimmed.is_empty() {
                columns[i].null_count += 1;
                continue;
            }

            let field_type = infer_field_type(trimmed);
            columns[i].inferred_type =
                merge_inferred_types(&columns[i].inferred_type, &field_type);
        }
    }

    Ok(CsvProfile {
        file_path: file_path.to_string(),
        row_count,
        column_count,
        columns,
    })
}

fn infer_field_type(value: &str) -> InferredType {
    if value.eq_ignore_ascii_case("true") || value.eq_ignore_ascii_case("false") {
        InferredType::Boolean
    } else if value.parse::<i64>().is_ok() {
        InferredType::Integer
    } else if value.parse::<f64>().is_ok() {
        InferredType::Float
    } else {
        InferredType::String
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
        (current_type, new_type) if std::mem::discriminant(current_type) == std::mem::discriminant(new_type) => current_type.clone(),
        _ => Mixed,
    }
}