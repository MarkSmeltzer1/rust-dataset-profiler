use std::error::Error;
use std::fs::File;
use std::sync::Arc;

use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::record::Field;
use tracing::info;

use crate::types::{ColumnProfile, InferredType, ParquetPreview, ParquetProfile};

pub fn preview_parquet(file_path: &str) -> Result<ParquetPreview, Box<dyn Error>> {
    info!("Opening Parquet file for dry run: {}", file_path);

    let file = File::open(file_path)?;
    let reader = SerializedFileReader::new(file)?;
    let metadata = reader.metadata();
    let schema = metadata.file_metadata().schema_descr();

    let columns: Vec<String> = schema
        .columns()
        .iter()
        .map(|col| col.name().to_string())
        .collect();

    let column_count = columns.len();

    info!("Dry run found {} Parquet columns", column_count);

    Ok(ParquetPreview {
        file_path: file_path.to_string(),
        column_count,
        columns,
    })
}

pub fn profile_parquet(file_path: &str) -> Result<ParquetProfile, Box<dyn Error>> {
    info!("Opening Parquet file for full profiling: {}", file_path);

    let file = File::open(file_path)?;
    let reader = Arc::new(SerializedFileReader::new(file)?);

    let metadata = reader.metadata();
    let schema = metadata.file_metadata().schema_descr();

    let column_names: Vec<String> = schema
        .columns()
        .iter()
        .map(|col| col.name().to_string())
        .collect();

    let column_count = column_names.len();

    let mut columns: Vec<ColumnProfile> = column_names
        .iter()
        .map(|name| ColumnProfile {
            name: name.clone(),
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

    let mut row_count = 0usize;
    let mut total_row_width = 0usize;

    let iter = reader.get_row_iter(None)?;

    for row_result in iter {
        let row = row_result?;
        row_count += 1;

        let mut row_width = 0usize;

        for (i, (_name, value)) in row.get_column_iter().enumerate() {
            let col = &mut columns[i];
            col.total_count += 1;

            match value {
                Field::Null => {
                    col.null_count += 1;
                }
                _ => {
                    col.non_null_count += 1;

                    let inferred = infer_parquet_field_type(value);
                    col.inferred_type = merge_inferred_types(&col.inferred_type, &inferred);

                    let rendered = render_field(value);
                    let len = rendered.len();

                    row_width += len;
                    update_length_stats(col, len);

                    if let Some(num) = extract_numeric(value) {
                        col.numeric_min = Some(match col.numeric_min {
                            Some(current) => current.min(num),
                            None => num,
                        });

                        col.numeric_max = Some(match col.numeric_max {
                            Some(current) => current.max(num),
                            None => num,
                        });

                        col.numeric_sum += num;
                        col.numeric_count += 1;
                    }
                }
            }
        }

        total_row_width += row_width;
    }

    info!("Processed {} Parquet rows", row_count);

    Ok(ParquetProfile {
        file_path: file_path.to_string(),
        row_count,
        column_count,
        columns,
        total_row_width,
    })
}

fn infer_parquet_field_type(field: &Field) -> InferredType {
    match field {
        Field::Null => InferredType::Unknown,
        Field::Bool(_) => InferredType::Boolean,
        Field::Byte(_)
        | Field::Short(_)
        | Field::Int(_)
        | Field::Long(_)
        | Field::UByte(_)
        | Field::UShort(_)
        | Field::UInt(_)
        | Field::ULong(_) => InferredType::Integer,
        Field::Float(_) | Field::Double(_) => InferredType::Float,
        Field::Str(_) => InferredType::String,
        _ => InferredType::Mixed,
    }
}

fn extract_numeric(field: &Field) -> Option<f64> {
    match field {
        Field::Byte(v) => Some(*v as f64),
        Field::Short(v) => Some(*v as f64),
        Field::Int(v) => Some(*v as f64),
        Field::Long(v) => Some(*v as f64),
        Field::UByte(v) => Some(*v as f64),
        Field::UShort(v) => Some(*v as f64),
        Field::UInt(v) => Some(*v as f64),
        Field::ULong(v) => Some(*v as f64),
        Field::Float(v) => Some(*v as f64),
        Field::Double(v) => Some(*v),
        _ => None,
    }
}

fn render_field(field: &Field) -> String {
    match field {
        Field::Null => "".to_string(),
        Field::Bool(v) => v.to_string(),
        Field::Byte(v) => v.to_string(),
        Field::Short(v) => v.to_string(),
        Field::Int(v) => v.to_string(),
        Field::Long(v) => v.to_string(),
        Field::UByte(v) => v.to_string(),
        Field::UShort(v) => v.to_string(),
        Field::UInt(v) => v.to_string(),
        Field::ULong(v) => v.to_string(),
        Field::Float(v) => v.to_string(),
        Field::Double(v) => v.to_string(),
        Field::Str(v) => v.clone(),
        other => format!("{:?}", other),
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