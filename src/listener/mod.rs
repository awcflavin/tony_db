use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::thread;

use crate::executor;

fn handle_client(mut stream: TcpStream) {
    let mut buffer = [0; 512];
    if let Ok(size) = stream.read(&mut buffer) {
        let message = String::from_utf8_lossy(&buffer[..size]);
        println!("Received message {}", message);
        let response = executor::execute_query(&message);
        let _ = stream.write_all(response.as_bytes());

        if message.trim() == "stop" {
            let _ = stream.write_all(b"Stopping the server as requested.");
            println!("Stopping the server as requested.");
            std::process::exit(0);
        }
    }
}

pub fn start_server() {
    let listener = TcpListener::bind("127.0.0.1:12345")
                    .expect("Failed to bind port");

    println!("listening on port 12345");

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {thread::spawn(||handle_client(stream));}
            Err(e) => eprintln!("Connection failed {}", e)
        }
    }
}