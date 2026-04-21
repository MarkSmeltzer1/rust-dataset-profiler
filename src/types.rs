#[derive(Debug, Clone)]
pub enum InferredType {
    Unknown,
    Integer,
    Float,
    Boolean,
    String,
    Mixed,
}

#[derive(Debug)]
pub struct ColumnProfile {
    pub name: String,
    pub null_count: usize,
    pub total_count: usize,
    pub inferred_type: InferredType,

    pub numeric_min: Option<f64>,
    pub numeric_max: Option<f64>,

    pub min_length: Option<usize>,
    pub max_length: Option<usize>,
    pub total_length: usize,
    pub non_null_count: usize,
}

#[derive(Debug)]
pub struct MalformedRowInfo {
    pub row_number: usize,
    pub expected_fields: usize,
    pub found_fields: usize,
}

#[derive(Debug)]
pub struct CsvProfile {
    pub file_path: String,
    pub row_count: usize,
    pub column_count: usize,
    pub malformed_row_count: usize,
    pub malformed_rows: Vec<MalformedRowInfo>,
    pub columns: Vec<ColumnProfile>,
    pub total_row_width: usize,
}

#[derive(Debug)]
pub struct CsvPreview {
    pub file_path: String,
    pub column_count: usize,
    pub headers: Vec<String>,
}

#[derive(Debug)]
pub struct JsonProfile {
    pub file_path: String,
    pub row_count: usize,
    pub column_count: usize,
    pub malformed_row_count: usize,
    pub malformed_rows: Vec<usize>,
    pub columns: Vec<ColumnProfile>,
    pub total_row_width: usize,
}

#[derive(Debug)]
pub struct JsonPreview {
    pub file_path: String,
    pub column_count: usize,
    pub keys: Vec<String>,
}

#[derive(Debug)]
pub struct ParquetProfile {
    pub file_path: String,
    pub row_count: usize,
    pub column_count: usize,
    pub columns: Vec<ColumnProfile>,
    pub total_row_width: usize,
}

#[derive(Debug)]
pub struct ParquetPreview {
    pub file_path: String,
    pub column_count: usize,
    pub columns: Vec<String>,
}