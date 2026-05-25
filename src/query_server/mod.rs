use crate::config::QueryServerConfig;
use crate::database::DatabaseManager;
use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::thread;
use tokio::runtime::Runtime;

mod handlers;
mod parsing;
mod protocol;
mod response;

pub struct QueryServer {
    config: QueryServerConfig,
    db: Arc<DatabaseManager>,
}

impl QueryServer {
    pub fn new(config: QueryServerConfig, db: Arc<DatabaseManager>) -> Self {
        Self { config, db }
    }

    pub fn start(&self) -> std::io::Result<()> {
        if !self.config.enabled {
            println!("Query server disabled");
            return Ok(());
        }

        let listener = TcpListener::bind(&self.config.listen_address)?;
        println!("Query server listening on {}", self.config.listen_address);

        for stream in listener.incoming() {
            match stream {
                Ok(stream) => {
                    let db = Arc::clone(&self.db);
                    thread::spawn(move || {
                        let rt = Runtime::new().unwrap();
                        rt.block_on(async move {
                            handle_query_connection(stream, db).await;
                        });
                    });
                }
                Err(err) => {
                    eprintln!("Query server accept error: {}", err);
                }
            }
        }

        Ok(())
    }
}

async fn handle_query_connection(mut stream: TcpStream, db: Arc<DatabaseManager>) {
    let addr = stream.peer_addr().ok();
    println!("[QUERY] Client connected: {:?}", addr);

    let reader = match stream.try_clone() {
        Ok(cloned) => BufReader::new(cloned),
        Err(err) => {
            eprintln!("[QUERY] Failed to clone stream: {}", err);
            return;
        }
    };

    for line in reader.lines() {
        match line {
            Ok(query) => {
                println!("[QUERY] Received: {}", query);
                let response = handlers::process_query(&query, db.as_ref()).await;

                if let Err(err) = writeln!(stream, "{}", response) {
                    eprintln!("[QUERY] Failed to send response: {}", err);
                    break;
                }
            }
            Err(err) => {
                eprintln!("[QUERY] Read error: {}", err);
                break;
            }
        }
    }

    println!("[QUERY] Client disconnected: {:?}", addr);
}