use tokio;
use redis::AsyncCommands;
use redis::cmd;
use std::error::Error;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let (tx, rx) = tokio::sync::mpsc::channel(100);
    let rx: tokio::sync::Mutex<tokio::sync::mpsc::Receiver<String>> = tokio::sync::Mutex::new(rx);
    let rx:std::sync::Arc<tokio::sync::Mutex<tokio::sync::mpsc::Receiver<String>>> = std::sync::Arc::new(rx);

    // Spawn worker tasks
    for i in 0..10 {
        let rx = std::sync::Arc::clone(&rx);
        tokio::spawn(async move {
            worker(i, rx).await;
        });
    }

    // Main task: read from source Redis and send to workers
    let source_client = redis::Client::open("redis://production-kong-ratelimit-redis-ro.vqoaoy.ng.0001.use1.cache.amazonaws.com:6379")?;
    let mut source_con = source_client.get_multiplexed_async_connection().await?;

    loop {
        let data = source_con.get("some_key").await?;
        tx.send(data).await?;
    }
}

async fn worker(id: usize, rx: std::sync::Arc<tokio::sync::Mutex<tokio::sync::mpsc::Receiver<String>>>) {
    let target_client = redis::Client::open("redis://test-cli.v6f73m.ng.0001.apse2.cache.amazonaws.com:6379").unwrap();
    //let mut target_con = target_client.get_async_connection().await.unwrap();
    let mut target_con = target_client.get_multiplexed_async_connection().await.unwrap();

    loop {
        // Each worker tries to receive from the shared receiver
        let data = {
            let mut rx_guard = rx.lock().await;
            rx_guard.recv().await
        };

        match data {
            Some(value) => {
                println!("Worker {} processing value: {}", id, value);
                target_con.set("some_key", value).await.unwrap();
            }
            None => break, // Channel closed
        }
    }
}
