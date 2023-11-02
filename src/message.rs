use std::collections::HashMap;
use serde::{Serialize, Deserialize};
use snafu::ResultExt;
use crate::error::{DecodeMsgBinSnafu, EncodeMsgBinSnafu, Result};

#[derive(Debug, Serialize, Deserialize)]
pub struct Message {
    pub topic: String,
    pub queue_id: u32,
    pub timestamp: u64,
    pub payload: Option<String>,
    pub offset: Option<u64>,
    pub key: Option<String>,
    pub header: Option<HashMap<String, String>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DispatchMessage {
    pub topic: String,
    pub queue_id: u32,
    pub msg_offset: usize,
    pub msg_size: usize,
    pub index_offset: u64,
    pub timestamp: u64,
}

impl Message {
    // Encode the message into a binary format
    pub fn encode(&self) -> Result<Vec<u8>> {
        bincode::serialize(self).context(EncodeMsgBinSnafu)
    }

    // Decode a binary message into a CustomMessage
    pub fn decode(encoded: &[u8]) -> Result<Self> {
        bincode::deserialize(encoded).context(DecodeMsgBinSnafu)
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Deref;
    use crate::message::Message;

    #[tokio::test]
    pub async fn encode_and_decode() {
        let payload = "This is a custom message payload.".to_string();

        let message = Message {
            topic: "my_topic".to_string(),
            queue_id: 0,
            key: Some("message_key".to_string()),
            timestamp: 1631894400,
            payload: Some(payload),
            offset: None,
            header: None,
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
