use std::fs;
use std::path::PathBuf;
use memmap2::MmapMut;
use snafu::{location, Location, ResultExt};
use crate::error::Error::InvalidInput;
use crate::msg_index::MessageIndexUnit;
use crate::error::{Result, StdIOSnafu};
use crate::mapped_file_queue::MappedFileQueue;

pub struct CommitLog {
    mapped_file_queue: MappedFileQueue,
}

impl CommitLog {
    pub fn new(store_path: &str, max_file_size: u64) -> Result<Self> {
        let base_dir = PathBuf::from(store_path);
        let commit_log_dir = base_dir.join("commitlog");

        fs::create_dir_all(&commit_log_dir).context(StdIOSnafu)?;
        let read_dir = commit_log_dir.read_dir().unwrap();
        let file_num = read_dir.count();

        let mut mapped_file_queue = MappedFileQueue::new(
            commit_log_dir.as_path().to_str().unwrap(), max_file_size)?;
        if file_num != 0 {
            mapped_file_queue.recovery(|mmap: &MmapMut, offset: usize| {
                let size = std::mem::size_of::<usize>();
                if offset + size < mmap.len() {
                    let mut buffer: Vec<u8> = vec![0; size];
                    buffer.copy_from_slice(&mmap[offset..offset + size]);

                    let msg_len = usize::from_le_bytes(buffer.as_slice().try_into().unwrap());
                    if msg_len > 0 {
                        return Some(msg_len + size);
                    }
                }
                None
            });
        }

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