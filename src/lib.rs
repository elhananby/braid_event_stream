use eventsource_stream::EventStream;
use futures_util::StreamExt;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::time::Instant;
use thiserror::Error;
use tokio::sync::mpsc::unbounded_channel;
use tokio::sync::mpsc::UnboundedReceiver;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KalmanEstimates {
    pub obj_id: u32,
    pub frame: u32,
    pub timestamp: f32,
    pub x: f64,
    pub y: f64,
    pub z: f64,
    pub xvel: f64,
    pub yvel: f64,
    pub zvel: f64,
}

#[derive(Debug, Clone)]
pub enum BraidEvent {
    Birth(KalmanEstimates),
    Update(KalmanEstimates),
    Death { obj_id: u32 },
}

#[derive(Error, Debug)]
pub enum BraidEventError {
    #[error("HTTP request failed: {0}")]
    HttpError(#[from] reqwest::Error),
    #[error("Failed to parse JSON: {0}")]
    JsonError(#[from] serde_json::Error),
    #[error("Unknown event type: {0}")]
    UnknownEventType(String),
    #[error("Missing field in event data: {0}")]
    MissingField(String),
}

pub struct BraidEventStream {
    client: Client,
    url: String,
}

impl BraidEventStream {
    pub fn new(url: String) -> Self {
        Self {
            client: Client::new(),
            url,
        }
    }

    pub async fn stream_events(
        &self,
    ) -> Result<UnboundedReceiver<Result<BraidEvent, BraidEventError>>, BraidEventError> {
        let response = self
            .client
            .get(&self.url)
            .header("Accept", "text/event-stream")
            .send()
            .await?;

        let stream = EventStream::new(response.bytes_stream());
        let (tx, rx) = unbounded_channel();

        tokio::spawn(async move {
            tokio::pin!(stream);

            while let Some(event_result) = stream.next().await {
                match event_result {
                    Ok(event) => {
                        if event.event == "message" {
                            let _received_time = Instant::now();
                            match Self::parse_event(&event.data) {
                                Ok(braid_event) => {
                                    if tx.send(Ok(braid_event)).is_err() {
                                        break; // Receiver was dropped
                                    }
                                }
                                Err(e) => {
                                    if tx.send(Err(e)).is_err() {
                                        break; // Receiver was dropped
                                    }
                                }
                            }
                        }
                    }
                    Err(e) => {
                        log::error!("Error in event stream: {:?}", e);
                        // Optionally, implement retry logic here
                    }
                }
            }
        });

        Ok(rx)
    }

    fn parse_event(data: &str) -> Result<BraidEvent, BraidEventError> {
        let json_data: serde_json::Value = serde_json::from_str(data)?;

        if let Some(msg) = json_data.get("msg") {
            if let Some(obj) = msg.as_object() {
                if let Some((event_type, event_data)) = obj.iter().next() {
                    match event_type.as_str() {
                        "Birth" | "Update" => {
                            let estimates: KalmanEstimates =
                                serde_json::from_value(event_data.clone())?;
                            Ok(if event_type == "Birth" {
                                BraidEvent::Birth(estimates)
                            } else {
                                BraidEvent::Update(estimates)
                            })
                        }
                        "Death" => {
                            let obj_id = event_data["obj_id"].as_u64().ok_or_else(|| {
                                BraidEventError::MissingField("obj_id".to_string())
                            })? as u32;
                            Ok(BraidEvent::Death { obj_id })
                        }
                        _ => Err(BraidEventError::UnknownEventType(event_type.to_string())),
                    }
                } else {
                    Err(BraidEventError::MissingField(
                        "event type and data".to_string(),
                    ))
                }
            } else {
                Err(BraidEventError::MissingField("msg object".to_string()))
            }
        } else {
            Err(BraidEventError::MissingField("msg".to_string()))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_birth_event() {
        let data = r#"{"msg":{"Birth":{"obj_id":1,"frame":100,"timestamp":1.5,"x":1.0,"y":2.0,"z":3.0,"xvel":0.1,"yvel":0.2,"zvel":0.3}}}"#;
        let event = BraidEventStream::parse_event(data).unwrap();
        match event {
            BraidEvent::Birth(estimates) => {
                assert_eq!(estimates.obj_id, 1);
                assert_eq!(estimates.frame, 100);
                assert_eq!(estimates.timestamp, 1.5);
                assert_eq!(estimates.x, 1.0);
                assert_eq!(estimates.y, 2.0);
                assert_eq!(estimates.z, 3.0);
                assert_eq!(estimates.xvel, 0.1);
                assert_eq!(estimates.yvel, 0.2);
                assert_eq!(estimates.zvel, 0.3);
            }
            _ => panic!("Expected Birth event"),
        }
    }

    #[test]
    fn test_parse_update_event() {
        let data = r#"{"msg":{"Update":{"obj_id":2,"frame":101,"timestamp":2.5,"x":4.0,"y":5.0,"z":6.0,"xvel":0.4,"yvel":0.5,"zvel":0.6}}}"#;
        let event = BraidEventStream::parse_event(data).unwrap();
        match event {
            BraidEvent::Update(estimates) => {
                assert_eq!(estimates.obj_id, 2);
                assert_eq!(estimates.frame, 101);
                assert_eq!(estimates.timestamp, 2.5);
                assert_eq!(estimates.x, 4.0);
                assert_eq!(estimates.y, 5.0);
                assert_eq!(estimates.z, 6.0);
                assert_eq!(estimates.xvel, 0.4);
                assert_eq!(estimates.yvel, 0.5);
                assert_eq!(estimates.zvel, 0.6);
            }
            _ => panic!("Expected Update event"),
        }
    }

    #[test]
    fn test_parse_death_event() {
        let data = r#"{"msg":{"Death":{"obj_id":3}}}"#;
        let event = BraidEventStream::parse_event(data).unwrap();
        match event {
            BraidEvent::Death { obj_id } => {
                assert_eq!(obj_id, 3);
            }
            _ => panic!("Expected Death event"),
        }
    }

    #[test]
    fn test_parse_unknown_event() {
        let data = r#"{"msg":{"Unknown":{"obj_id":4}}}"#;
        let result = BraidEventStream::parse_event(data);
        assert!(matches!(result, Err(BraidEventError::UnknownEventType(_))));
    }
}
