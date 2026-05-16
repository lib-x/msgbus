use msgbus_sdk_rust::MsgbusClient;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let endpoint =
        std::env::var("MSGBUS_ENDPOINT").unwrap_or_else(|_| "http://127.0.0.1:50051".to_string());
    let mut client = MsgbusClient::connect(endpoint).await?;

    let published = client.publish("demo.events", b"hello msgbus").await?;
    println!("published head_id={}", published.head_id);

    let messages = client
        .fetch(
            "demo.events",
            published.origin.clone(),
            published.head_id,
            10,
        )
        .await?;
    println!("fetched {} message(s)", messages.len());

    Ok(())
}
