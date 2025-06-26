use std::env;
use std::io::{self, Write};
use std::path::PathBuf;
use std::time::Duration;

use zookeeper_client::Error::NodeExists;
use zookeeper_client::{Acls, Client, CreateMode, TlsOptions};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    smol::block_on(run()).unwrap_or_else(|e| {
        eprintln!("Error: {}", e);
        std::process::exit(1);
    });
    Ok(())
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let connect_string = env::var("ZK_CONNECT_STRING").unwrap_or_else(|_| "tcp+tls://localhost:2281".to_string());
    let ca_cert = PathBuf::from(env::var("ZK_CA_CERT").expect("ZK_CA_CERT environment variable is required"));
    let client_cert =
        PathBuf::from(env::var("ZK_CLIENT_CERT").expect("ZK_CLIENT_CERT environment variable is required"));
    let client_key = PathBuf::from(env::var("ZK_CLIENT_KEY").expect("ZK_CLIENT_KEY environment variable is required"));

    println!("Connecting to ZooKeeper with file-based TLS...");
    println!("Server: {}", connect_string);
    println!("CA cert: {}", ca_cert.display());
    println!("Client cert: {}", client_cert.display());
    println!("Client key: {}", client_key.display());

    let loaded_ca_cert = async_fs::read_to_string(&ca_cert).await?;
    let tls_options = TlsOptions::default()
        .with_pem_ca_certs(&loaded_ca_cert)?
        .with_pem_identity_files(&client_cert, &client_key)
        .await?;

    let tls_options = unsafe { tls_options.with_no_hostname_verification() };

    println!("WARNING: Hostname verification disabled!");

    let client = Client::connector()
        .connection_timeout(Duration::from_secs(10))
        .session_timeout(Duration::from_secs(30))
        .tls(tls_options)
        .secure_connect(&connect_string)
        .await?;

    println!("Connected to ZooKeeper successfully!");

    let path = "/tls_example";

    loop {
        print!("\nOptions:\ne. Edit key\nq. Quit\nEnter choice (e/q): ");
        io::stdout().flush()?;

        let mut input = String::new();
        io::stdin().read_line(&mut input)?;

        match input.trim() {
            "e" => {
                print!("Enter new data for the key: ");
                io::stdout().flush()?;

                let mut data_input = String::new();
                io::stdin().read_line(&mut data_input)?;
                let data = data_input.trim().as_bytes();

                println!("Setting data at path: {}", path);
                match client.create(path, data, &CreateMode::Ephemeral.with_acls(Acls::anyone_all())).await {
                    Ok(_) => println!("ZNode created successfully"),
                    Err(NodeExists) => {
                        println!("ZNode already exists, updating data...");
                        client.set_data(path, data, None).await?;
                        println!("ZNode data updated successfully");
                    },
                    Err(e) => {
                        println!("Error creating/updating ZNode: {}", e);
                        continue;
                    },
                }

                match client.get_data(path).await {
                    Ok((data, _stat)) => {
                        println!("Current data: {}", String::from_utf8_lossy(&data));
                    },
                    Err(e) => println!("Error reading data: {}", e),
                }
            },
            "q" => {
                println!("Cleaning up and exiting...");
                match client.delete(path, None).await {
                    Ok(_) => println!("ZNode deleted successfully"),
                    Err(_) => println!("ZNode may not exist or already deleted"),
                }
                break;
            },
            _ => {
                println!("Invalid choice. Please enter 'e' or 'q'.");
            },
        }
    }

    println!("Example completed successfully!");
    Ok(())
}
