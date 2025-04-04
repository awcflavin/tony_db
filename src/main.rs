use clap::{Parser, Subcommand};
use std::net::{TcpStream};

mod client;
mod listener;

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
            if TcpStream::connect("127.0.0.1:12345").is_ok() {
                println!("TonyDB service is already running.");
                return;
            }

            #[cfg(target_os = "windows")]
            {
                use std::os::windows::process::CommandExt;
                const DETACHED_PROCESS: u32 = 0x00000008;
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
            start_service();
        }
        Commands::Query { query } => {
            send_command(&query);
        }
        Commands::Stop => {
            send_command(&"stop".to_string());
        }
    }
}

fn start_service() {
    listener::main();
    println!("TonyDB service is active...");
}

fn send_command(command: &String) {
    client::send_command(command)
}