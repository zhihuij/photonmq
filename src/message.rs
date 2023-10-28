use serde::{Serialize, Deserialize};

#[derive(Debug, Serialize, Deserialize)]
struct MessageHeader {
    topic: String,
    partition: i32,
    key: Option<String>,
    timestamp: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Message {
    header: MessageHeader,
    payload: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ConsumeMessage {
    pub topic: String,
    pub partition: u32,
    pub offset: u64,
}

impl Message {
    // Encode the message into a binary format
    pub fn encode(&self) -> bincode::Result<Vec<u8>> {
        bincode::serialize(self)
    }

    // Decode a binary message into a CustomMessage
    pub fn decode(encoded: &[u8]) -> bincode::Result<Self> {
        bincode::deserialize(encoded)
    }

    pub fn encode_json(&self) -> serde_json::Result<String> {
        serde_json::to_string(&self)
    }

    pub fn decode_json(encoded: &str) -> serde_json::Result<Message> {
        serde_json::from_str(encoded)
    }
}

impl ConsumeMessage {
    pub fn decode_json(encoded: &str) -> Option<Self> {
        serde_json::from_str(encoded).unwrap()
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Deref;
    use crate::message::{Message, MessageHeader};

    #[tokio::test]
    pub async fn encode_and_decode() {
        // Create a sample CustomMessage
        let header = MessageHeader {
            topic: "my_topic".to_string(),
            partition: 0,
            key: Some("message_key".to_string()),
            timestamp: 1631894400,
        };

        let payload = "This is a custom message payload.".to_string();

        let message = Message {
            header,
            payload,
        };

        // Encode the message into a binary format
        let encoded_message = message.encode();

        // Decode the binary message into a CustomMessage
        let decode_result = Message::decode(encoded_message.unwrap().deref());
        match decode_result {
            Ok(decode_msg) => { println!("{:?}", decode_msg); }
            Err(error) => { println!("Failed to decode the message {:?}", error); }
        }
    }
}
