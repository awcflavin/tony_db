use clap::{Parser, Subcommand};
use std::fs::{self, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::thread;
use std::time::Duration;

const COMMAND_FILE: &str = "tonydb_commands.txt";
const RESPONSE_FILE: &str = "tonydb_responses.txt";

#[derive(Parser)]
#[command(name = "TonyDB")]
#[command(about = "An in-memory relational database", version = "0.1.0")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Init,
    Query {
        query: String,
    },
    Stop,
    #[command(hide = true)]
    RunService,
}

fn main() {
    let cli = Cli::parse();

    match cli.command {
        Commands::Init => {
            #[cfg(target_os = "windows")]
            {
                use std::os::windows::process::CommandExt;
                const DETACHED_PROCESS: u32 = 0x00000008;
                if is_service_running() {
                    println!("TonyDB service is already running.");
                    return;
                }
                let current_exe = std::env::current_exe().expect("Failed to get current exe");
                let _child = std::process::Command::new(current_exe)
                    .arg("run-service")
                    .creation_flags(DETACHED_PROCESS)
                    .spawn()
                    .expect("Failed to spawn background service");
                println!("TonyDB service started in background.");
            }
            #[cfg(not(target_os = "windows"))]
            {
                unimplemented!("This service is implemented only for Windows.");
            }
        }
        Commands::RunService => {
            fs::File::create(COMMAND_FILE).expect("Failed to create command file");
            fs::File::create(RESPONSE_FILE).expect("Failed to create response file");
            start_service();
        }
        Commands::Query { query } => {
            if !is_service_running() {
                println!("Error: TonyDB service is not running.");
                return;
            }
            send_command(format!("query {}", query));
            thread::sleep(Duration::from_millis(200));
            let response = read_response();
            println!("{}", response);
        }
        Commands::Stop => {
            if !is_service_running() {
                println!("Error: TonyDB service is not running.");
                return;
            }
            send_command("stop".to_string());
            for _ in 0..50 {
                if !is_service_running() {
                    println!("TonyDB service stopped.");
                    return;
                }
                thread::sleep(Duration::from_millis(100));
            }
            println!("Stop command issued, but the service did not stop in time.");
        }
    }
}

fn start_service() {
    println!("TonyDB service is active...");

    // Main service loop to process commands.
    loop {
        if let Some(command) = read_command() {
            if command.starts_with("query ") {
                println!("Processing query: {}", &command[6..]);
                write_response("query executed".to_string());
            } else if command.trim() == "stop" {
                println!("Stopping TonyDB service...");
                if std::path::Path::new(COMMAND_FILE).exists() {
                    fs::remove_file(COMMAND_FILE)
                        .expect("Failed to delete command file");
                }
                if std::path::Path::new(RESPONSE_FILE).exists() {
                    fs::remove_file(RESPONSE_FILE)
                        .expect("Failed to delete response file");
                }
                break;
            }
        } else {
            thread::sleep(Duration::from_millis(100));
        }
    }
}

fn send_command(command: String) {
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(COMMAND_FILE)
        .expect("Failed to open command file");
    writeln!(file, "{}", command).expect("Failed to write command");
    file.flush().expect("Failed to flush command file");
}

fn read_command() -> Option<String> {
    if let Ok(file) = OpenOptions::new().read(true).open(COMMAND_FILE) {
        let reader = BufReader::new(file);
        let mut lines: Vec<String> = reader.lines().filter_map(Result::ok).collect();
        if !lines.is_empty() {
            let command = lines.remove(0);
            let mut file = OpenOptions::new()
                .write(true)
                .truncate(true)
                .open(COMMAND_FILE)
                .expect("Failed to open command file for truncation");
            for line in lines {
                writeln!(file, "{}", line).expect("Failed to write remaining commands");
            }
            return Some(command);
        }
    }
    None
}

fn write_response(response: String) {
    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(RESPONSE_FILE)
        .expect("Failed to open response file");
    writeln!(file, "{}", response).expect("Failed to write response");
    file.flush().expect("Failed to flush response file");
}

fn read_response() -> String {
    if let Ok(file) = OpenOptions::new().read(true).open(RESPONSE_FILE) {
        let reader = BufReader::new(file);
        for line in reader.lines() {
            if let Ok(response) = line {
                return response;
            }
        }
    }
    "No response from service.".to_string()
}

fn is_service_running() -> bool {
    // The existence of the command file indicates the service is running.
    std::path::Path::new(COMMAND_FILE).exists()
}
