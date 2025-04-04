use std::io::{Read, Write};
use std::net::TcpStream;

pub fn send_command(command: &str) {
    let mut stream = TcpStream::connect("127.0.0.1:12345")
                        .expect("Could not connect to database server");
    stream.write_all(command.as_bytes()).unwrap();

    let mut buffer = [0; 512];
    let size = stream.read(&mut buffer).unwrap();
    println!("Response: {}", String::from_utf8_lossy(&buffer[..size]));
}

pub fn send_query(query: &str) {
    let mut stream = TcpStream::connect("127.0.0.1:12345")
                        .expect("Could not connect to database server");
    let json_query = format!(r#"{{"query": "{}"}}"#, query);
    stream.write_all(json_query.as_bytes()).unwrap();

    let mut buffer = [0; 512];
    let size = stream.read(&mut buffer).unwrap();
    println!("Response: {}", String::from_utf8_lossy(&buffer[..size]));
}