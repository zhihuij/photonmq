use snafu::{location, Location};
use crate::error::Error::InvalidInput;
use crate::msg_index::MessageIndexUnit;
use crate::error::Result;
use crate::mapped_file_queue::MappedFileQueue;

pub struct CommitLog {
    mapped_file_queue: MappedFileQueue,
}

impl CommitLog {
    pub fn open(store_path: &str, max_file_size: u64) -> Result<Self> {
        let mapped_file_queue = MappedFileQueue::open(store_path, max_file_size)?;
        Ok(CommitLog { mapped_file_queue })
    }

    pub fn write_records(&mut self, data: &Vec<u8>) -> Result<usize> {
        // Write the record to the current file
        self.mapped_file_queue.append(data)
    }

    pub fn read_records(&self, msg_index_unit: &MessageIndexUnit) -> Result<Vec<u8>> {
        if msg_index_unit.size > 0 {
            // Read and return records
            self.mapped_file_queue.read(
                msg_index_unit.offset as usize, msg_index_unit.size as usize)
        } else {
            Err(InvalidInput {
                location: location!(),
                msg: "Buffer size doesn't match the mapped region's size.".to_string(),
            })
        }
    }
}