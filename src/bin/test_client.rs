use serde_json::json;
use std::env;
use std::io::{Write, Read};
use std::net::TcpStream;
use std::time::Duration;

#[derive(Debug)]
struct MessageData {
    addresses: Addresses,
    bsc_mints: Vec<String>,
    chat_id: i64,
    chat_name: String,
    ethereum_mints: Vec<String>,
    evm_mints: Vec<String>,
    images: Vec<String>,
    links: Vec<String>,
    mints: Vec<String>,
    solana_mints: Vec<String>,
    text: String,
    tokens: Vec<String>,
}

#[derive(Debug)]
struct Addresses {
    bsc: Vec<String>,
    ethereum: Vec<String>,
    solana: Vec<String>,
    unknown: Vec<String>,
}

impl MessageData {
    fn new(mint_address: &str) -> Self {
        MessageData {
            addresses: Addresses {
                bsc: vec![],
                ethereum: vec![mint_address.to_string()],
                solana: vec![],
                unknown: vec![],
            },
            bsc_mints: vec![],
            chat_id: -1001441780077,
            chat_name: "MadApes".to_string(),
            ethereum_mints: vec![mint_address.to_string()],
            evm_mints: vec![mint_address.to_string()],
            images: vec![],
            links: vec![format!("https://www.livo.trade/token/{}", mint_address)],
            mints: vec![mint_address.to_string()],
            solana_mints: vec![],
            text: format!("Rebecca tweet about this cat\n\nhttps://www.livo.trade/token/{}", mint_address),
            tokens: vec![],
        }
    }

    fn to_json_string(&self) -> String {
        let json_value = json!({
            "addresses": {
                "bsc": self.addresses.bsc,
                "ethereum": self.addresses.ethereum,
                "solana": self.addresses.solana,
                "unknown": self.addresses.unknown
            },
            "bsc_mints": self.bsc_mints,
            "chat_id": self.chat_id,
            "chat_name": self.chat_name,
            "ethereum_mints": self.ethereum_mints,
            "evm_mints": self.evm_mints,
            "images": self.images,
            "links": self.links,
            "mints": self.mints,
            "solana_mints": self.solana_mints,
            "text": self.text,
            "tokens": self.tokens
        });
        
        json_value.to_string()
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    
    if args.len() < 3 {
        eprintln!("Usage: {} <TCP_ADDRESS:PORT> <MINT_ADDRESS>", args[0]);
        eprintln!("Example: {} 127.0.0.1:8080 0xf7e1d005e607edce656ab42bb81ffd1f4ea81110", args[0]);
        std::process::exit(1);
    }
    
    let tcp_address = &args[1];
    let mint_address = &args[2];
    
    // Validate mint address format (basic check)
    if mint_address.len() != 42 || !mint_address.starts_with("0x") {
        eprintln!("Warning: Mint address doesn't look like a valid Ethereum address (should start with 0x and be 42 chars)");
    }
    
    println!("Connecting to {}...", tcp_address);
    
    // Connect to TCP server with timeout
    let mut stream = TcpStream::connect(tcp_address)?;
    stream.set_write_timeout(Some(Duration::from_secs(5)))?;
    stream.set_read_timeout(Some(Duration::from_secs(5)))?;
    
    println!("Connected! Sending message for mint address: {}", mint_address);
    
    // Create message
    let message = MessageData::new(mint_address);
    let json_string = message.to_json_string();
    
    println!("Sending JSON payload:");
    println!("{}", json_string);
    
    // Send message with newline delimiter (common in TCP protocols)
    let send_data = format!("{}\n", json_string);
    stream.write_all(send_data.as_bytes())?;
    stream.flush()?;
    
    println!("Message sent successfully!");
    
    // Optional: Read response
    let mut buffer = [0u8; 4096];
    match stream.read(&mut buffer) {
        Ok(n) if n > 0 => {
            let response = String::from_utf8_lossy(&buffer[..n]);
            println!("Received response: {}", response);
        }
        Ok(_) => {
            println!("No response from server");
        }
        Err(e) => {
            eprintln!("Error reading response: {}", e);
        }
    }
    
    Ok(())
}