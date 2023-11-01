use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use snafu::{location, Location};
use crate::error::Error::InvalidInput;

use crate::msg_index::MessageIndexUnit;
use crate::mmap_file::MemoryMappedFile;
use crate::error::Result;

pub struct CommitLog {
    current_file: Arc<RwLock<MemoryMappedFile>>,
}

impl CommitLog {
    pub fn open(base_path: &str, max_file_size: u64) -> Result<Self> {
        let base_dir = PathBuf::from(base_path);
        let msg_log_path = base_dir.join("test.log");
        // Create or open the initial MemoryMappedFile
        let current_file = Arc::new(RwLock::new(
            MemoryMappedFile::open(msg_log_path.to_str().unwrap(), max_file_size)?));

        Ok(CommitLog {
            current_file
        })
    }

    pub fn write_records(&self, data: &Vec<u8>) -> Result<usize> {
        let mut current_file = self.current_file.write().unwrap();

        // Write the record to the current file
        current_file.append(data)
    }

    pub fn read_records(&self, msg_index_unit: &MessageIndexUnit) -> Result<Vec<u8>> {
        if msg_index_unit.size > 0 {
            // Lock the RwLock for reading
            let current_file = self.current_file.read().unwrap();
            // Read and return records
            let records = current_file.read(msg_index_unit.offset as usize,
                                            msg_index_unit.size as usize).unwrap();

            Ok(records)
        } else {
            Err(InvalidInput {
                location: location!(),
                msg: "Buffer size doesn't match the mapped region's size.".to_string(),
            })
        }
    }
}