use std::fs;
use std::process::Command;

fn dprofile_command() -> Command {
    Command::new(env!("CARGO_BIN_EXE_dataset-profiler"))
}

fn temp_csv(name: &str) -> String {
    let path = std::env::temp_dir().join(name);
    fs::write(&path, "id,name,age\n1,Alice,25\n2,Bob,30\n3,,22\n")
        .expect("test CSV should be written");
    path.to_string_lossy().to_string()
}

#[test]
fn missing_file_exits_with_error_message() {
    let output = dprofile_command()
        .args(["--file", "missing.csv"])
        .output()
        .expect("command should run");

    assert!(!output.status.success());

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("File not found"));
    assert!(stderr.contains("Tip:"));
}

#[test]
fn unsupported_format_exits_with_error_message() {
    let csv_path = temp_csv("dprofile_cli_bad_format.csv");

    let output = dprofile_command()
        .args(["--file", &csv_path, "--format", "xml"])
        .output()
        .expect("command should run");

    assert!(!output.status.success());

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("Unsupported file format"));
    assert!(stderr.contains("csv"));
    assert!(stderr.contains("parquet"));

    let _ = fs::remove_file(csv_path);
}

#[test]
fn invalid_threads_exits_with_error_message() {
    let csv_path = temp_csv("dprofile_cli_bad_threads.csv");

    let output = dprofile_command()
        .args(["--file", &csv_path, "--threads", "0"])
        .output()
        .expect("command should run");

    assert!(!output.status.success());

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("Invalid value for --threads"));
    assert!(stderr.contains("at least 1"));

    let _ = fs::remove_file(csv_path);
}

#[test]
fn valid_threads_setting_is_logged_in_verbose_mode() {
    let csv_path = temp_csv("dprofile_cli_threads.csv");

    let output = dprofile_command()
        .args(["--file", &csv_path, "--threads", "2", "--verbose"])
        .output()
        .expect("command should run");

    assert!(output.status.success());

    let combined_output = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(combined_output.contains("Thread setting: 2"));

    let _ = fs::remove_file(csv_path);
}

#[test]
fn help_includes_threads_flag() {
    let output = dprofile_command()
        .arg("--help")
        .output()
        .expect("command should run");

    assert!(output.status.success());

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("--threads"));
}

#[test]
fn invalid_config_exits_with_error_message() {
    let csv_path = temp_csv("dprofile_cli_invalid_config.csv");
    let config_path = std::env::temp_dir().join("dprofile_invalid_config.toml");
    fs::write(&config_path, "format = [").expect("test config should be written");

    let output = dprofile_command()
        .args([
            "--file",
            &csv_path,
            "--config",
            config_path
                .to_str()
                .expect("temp path should be valid UTF-8"),
        ])
        .output()
        .expect("command should run");

    assert!(!output.status.success());

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("Failed to load config file"));
    assert!(stderr.contains("valid TOML"));

    let _ = fs::remove_file(config_path);
    let _ = fs::remove_file(csv_path);
}

#[test]
fn config_verbose_enables_info_logging() {
    let csv_path = temp_csv("dprofile_cli_verbose_config.csv");
    let config_path = std::env::temp_dir().join("dprofile_verbose_config.toml");
    fs::write(
        &config_path,
        r#"
format = "csv"
delimiter = ","
verbose = true
dry_run = false
"#,
    )
    .expect("test config should be written");

    let output = dprofile_command()
        .args([
            "--file",
            &csv_path,
            "--config",
            config_path
                .to_str()
                .expect("temp path should be valid UTF-8"),
        ])
        .output()
        .expect("command should run");

    assert!(output.status.success());

    let combined_output = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(combined_output.contains("Dataset profiler starting"));
    assert!(combined_output.contains("Config loaded"));

    let _ = fs::remove_file(config_path);
    let _ = fs::remove_file(csv_path);
}

#[test]
fn cli_format_overrides_config_format() {
    let csv_path = temp_csv("dprofile_cli_format_override.csv");
    let config_path = std::env::temp_dir().join("dprofile_format_override_config.toml");
    fs::write(
        &config_path,
        r#"
format = "json"
delimiter = ","
verbose = false
dry_run = false
threads = 1
"#,
    )
    .expect("test config should be written");

    let output = dprofile_command()
        .args([
            "--file",
            &csv_path,
            "--config",
            config_path
                .to_str()
                .expect("temp path should be valid UTF-8"),
            "--format",
            "csv",
        ])
        .output()
        .expect("command should run");

    assert!(output.status.success());

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("CSV Profile Summary"));
    assert!(stdout.contains("Format: csv"));

    let _ = fs::remove_file(config_path);
    let _ = fs::remove_file(csv_path);
}
