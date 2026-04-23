use std::fs;
use std::io::{BufRead, BufReader};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::Path;

const SOCKET_PATH: &str = "/tmp/solana_listener.sock";

fn handle_client(stream: UnixStream) {
    println!("Client connected");

    let reader = BufReader::new(stream);
    for line in reader.lines() {
        match line {
            Ok(msg) => println!("MESSAGE: {}", msg),
            Err(err) => {
                eprintln!("Read error: {}", err);
                break;
            }
        }
    }

    println!("Client disconnected");
}

fn main() -> std::io::Result<()> {
    if Path::new(SOCKET_PATH).exists() {
        fs::remove_file(SOCKET_PATH)?;
    }

    let listener = UnixListener::bind(SOCKET_PATH)?;
    println!("Listening on unix socket: {}", SOCKET_PATH);

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => handle_client(stream),
            Err(err) => eprintln!("Accept error: {}", err),
        }
    }

    Ok(())
}