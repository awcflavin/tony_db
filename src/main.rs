use clap::{Parser, Subcommand};
use std::net::{TcpStream};
use std::process::Command;
use std::thread;
use tony_db;

mod client;

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
        Commands::RunService => {
            // hidden arg to start the service
            tony_db::listener::start_server();
        }
        Commands::Init => {
            start_background_service();
        }
        Commands::Query { query } => {
            send_command(&query);
        }
        Commands::Stop => {
            send_command(&"stop".to_string());
        }
    }
}

fn start_background_service() {
    
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        const DETACHED_PROCESS: u32 = 0x00000008; // detached process code
        let current_exe = std::env::current_exe().expect("Failed to get current executable path");

        Command::new(current_exe)
            .arg("run-service")
            .creation_flags(DETACHED_PROCESS)
            .spawn()
            .expect("Failed to start background service");

        thread::sleep(std::time::Duration::from_millis(100));
    }

    #[cfg(not(target_os = "windows"))]
    {
        unimplemented!("This service is implemented only for Windows.");
    }
}

fn send_command(command: &String) {
    client::send_command(command)
}