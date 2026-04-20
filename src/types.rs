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
}