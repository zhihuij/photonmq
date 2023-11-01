use std::collections::HashMap;
use std::path::PathBuf;
use crate::config::ConfigOptions;
use crate::message::DispatchMessage;
use crate::msg_index::{MessageIndex, MessageIndexUnit};
use crate::error::Result;

pub struct IndexStore {
    config: ConfigOptions,
    index_map: HashMap<String, HashMap<u32, MessageIndex>>,
}

impl IndexStore {
    // Constructor: Open or create a file for index store.
    pub fn open(config: ConfigOptions) -> Result<Self> {
        Ok(IndexStore { config, index_map: HashMap::new() })
    }

    pub fn put_msg_index(&mut self, dispatch_msg: &DispatchMessage) -> Result<u64> {
        let msg_index = self.find_or_create_index(
            dispatch_msg.topic.as_str(), dispatch_msg.queue_id);
        msg_index.put_msg_index(dispatch_msg.index_offset, dispatch_msg.msg_offset, dispatch_msg.msg_size)
    }

    pub fn read_msg_index(&mut self, topic: &str, queue_id: u32, index_offset: u64) -> Result<MessageIndexUnit> {
        let msg_index = self.find_or_create_index(topic, queue_id);
        msg_index.read_msg_index(index_offset)
    }

    fn find_or_create_index(&mut self, topic: &str, queue_id: u32) -> &mut MessageIndex {
        let topic_index_map = self.index_map.entry(topic.to_string()).or_insert_with(|| HashMap::new());

        topic_index_map.entry(queue_id).or_insert_with(|| {
            let msg_store_path_clone = self.config.msg_store_path.clone();
            let base_dir = PathBuf::from(msg_store_path_clone);
            let index_store_path = base_dir.join("index");
            MessageIndex::open(
                index_store_path.as_path().to_str().unwrap(),
                topic, queue_id, self.config.index_file_size).unwrap()
        })
    }
}