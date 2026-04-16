#[derive(Debug)]
pub struct CsvProfile {
    pub file_path: String,
    pub row_count: usize,
    pub column_count: usize,
    pub headers: Vec<String>,
    pub null_count: usize,
}