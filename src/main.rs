use std::io::{BufRead, BufReader};
use std::net::{TcpListener, TcpStream};

fn handle_client(stream: TcpStream) {
    let addr = stream.peer_addr().ok();
    println!("Client connected: {:?}", addr);

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

    println!("Client disconnected: {:?}", addr);
}

fn main() -> std::io::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:9000")?;
    println!("Listening on tcp://127.0.0.1:9000");

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => handle_client(stream),
            Err(err) => eprintln!("Accept error: {}", err),
        }
    }

    Ok(())
}