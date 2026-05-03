use std::fs;
use std::hint::black_box;
use std::path::PathBuf;

use criterion::{criterion_group, criterion_main, Criterion};
use dataset_profiler::readers::{csv, json};

fn fixture_path(name: &str) -> PathBuf {
    std::env::temp_dir().join(name)
}

fn write_csv_fixture(name: &str, rows: usize) -> String {
    let path = fixture_path(name);
    let mut content = String::from("id,name,amount,active\n");

    for i in 0..rows {
        let active = if i % 2 == 0 { "true" } else { "false" };
        content.push_str(&format!(
            "{},user_{},{:.2},{}\n",
            i,
            i,
            i as f64 * 1.25,
            active
        ));
    }

    fs::write(&path, content).expect("CSV benchmark fixture should be written");
    path.to_string_lossy().to_string()
}

fn write_json_fixture(name: &str, rows: usize) -> String {
    let path = fixture_path(name);
    let mut content = String::from("[\n");

    for i in 0..rows {
        let comma = if i + 1 == rows { "" } else { "," };
        let active = if i % 2 == 0 { "true" } else { "false" };
        content.push_str(&format!(
            r#"  {{"id": {}, "name": "user_{}", "amount": {:.2}, "active": {}}}{}"#,
            i,
            i,
            i as f64 * 1.25,
            active,
            comma
        ));
        content.push('\n');
    }

    content.push_str("]\n");
    fs::write(&path, content).expect("JSON benchmark fixture should be written");
    path.to_string_lossy().to_string()
}

fn benchmark_csv(c: &mut Criterion) {
    let small = write_csv_fixture("dprofile_bench_small.csv", 100);
    let medium = write_csv_fixture("dprofile_bench_medium.csv", 10_000);

    c.bench_function("profile_csv_100_rows", |b| {
        b.iter(|| csv::profile_csv(black_box(&small), black_box(b',')).unwrap())
    });

    c.bench_function("profile_csv_10000_rows", |b| {
        b.iter(|| csv::profile_csv(black_box(&medium), black_box(b',')).unwrap())
    });
}

fn benchmark_json(c: &mut Criterion) {
    let small = write_json_fixture("dprofile_bench_small.json", 100);
    let medium = write_json_fixture("dprofile_bench_medium.json", 10_000);

    c.bench_function("profile_json_100_rows", |b| {
        b.iter(|| json::profile_json(black_box(&small)).unwrap())
    });

    c.bench_function("profile_json_10000_rows", |b| {
        b.iter(|| json::profile_json(black_box(&medium)).unwrap())
    });
}

criterion_group!(benches, benchmark_csv, benchmark_json);
criterion_main!(benches);
