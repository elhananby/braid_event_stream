use braid_event_stream::BraidEventStream;
use std::error::Error;
use tokio;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let events_url = String::from("http://10.40.80.6:8397/events");
    let braid_stream = BraidEventStream::new(events_url);
    let mut event_receiver = braid_stream.stream_events().await?;

    while let Some(event_result) = event_receiver.recv().await {
        match event_result {
            Ok(event) => {
                println!("Received event: {:?}", event);
            }
            Err(e) => {
                eprintln!("Error receiving event: {:?}", e);
            }
        }
    }

    Ok(())
}
