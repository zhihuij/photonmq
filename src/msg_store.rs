use std::sync::{Arc, Mutex};
use crate::commit_log::CommitLog;
use crate::config::ConfigOptions;
use crate::index_store::IndexStore;
use crate::message::{DispatchMessage, Message};
use crate::error::Result;

pub struct MessageStore {
    commit_log: Arc<Mutex<CommitLog>>,
    index_store: Arc<Mutex<IndexStore>>,
}

impl MessageStore {
    // Constructor: Open or create a file for message store.
    pub fn open(config: &ConfigOptions) -> Result<Self> {
        let commit_log = Arc::new(Mutex::new(CommitLog::open(
            config.msg_store_path.as_str(), config.msg_store_file_size)?));
        let config_clone = config.clone();
        let index_store = Arc::new(Mutex::new(IndexStore::open(config_clone)?));

        Ok(MessageStore { commit_log, index_store })
    }

    pub async fn write_msg(&self, msg: Message) -> Result<usize> {
        // write the msg
        let commit_log = self.commit_log.lock().unwrap();
        let encoded_msg = msg.encode()?;
        let msg_offset = commit_log.write_records(&encoded_msg)?;

        let mut index_store = self.index_store.lock().unwrap();
        let dispatch_msg = DispatchMessage {
            topic: msg.topic.clone(),
            queue_id: msg.queue_id,
            msg_offset,
            msg_size: encoded_msg.len(),
            timestamp: msg.timestamp,
        };

        //TODO generate the message index, use channel
        index_store.put_msg_index(&dispatch_msg)
    }

    pub async fn read_msg(&self, consume_msg: Message) -> Result<Vec<Message>> {
        let commit_log = self.commit_log.lock().unwrap();
        let mut index_store = self.index_store.lock().unwrap();

        let index_query_result = index_store.read_msg_index(
            consume_msg.topic.as_str(),
            consume_msg.queue_id,
            consume_msg.offset.unwrap());

        match index_query_result {
            Ok(msg_index_unit) => {
                let msg_content = commit_log.read_records(&msg_index_unit)?;
                let mut result_vec = Vec::new();
                let msg = Message::decode(msg_content.as_slice())?;

                result_vec.push(msg);

                Ok(result_vec)
            }
            Err(err) => {
                Err(err)
            }
        }
    }
}