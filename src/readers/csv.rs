use std::error::Error;
use std::fs::File;

use csv::StringRecord;

use crate::types::CsvProfile;

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
    let mut null_count = 0usize;

    for result in reader.records() {
        let record: StringRecord = result?;
        row_count += 1;

        for field in &record {
            if field.trim().is_empty() {
                null_count += 1;
            }
        }
    }

    Ok(CsvProfile {
        file_path: file_path.to_string(),
        row_count,
        column_count,
        headers,
        null_count,
    })
}