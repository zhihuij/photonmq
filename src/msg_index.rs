use std::path::PathBuf;
use std::{fs, u32, u64, usize};
use memmap2::MmapMut;
use snafu::ResultExt;
use crate::error::{Result, StdIOSnafu};
use crate::mapped_file_queue::MappedFileQueue;

pub struct MessageIndex {
    mapped_file_queue: MappedFileQueue,
}

pub const MSG_INDEX_UNIT_SIZE: usize = std::mem::size_of::<MessageIndexUnit>();

pub struct MessageIndexUnit {
    pub offset: u64,
    pub size: u32,
}

impl MessageIndex {
    // Constructor: Open or create a file for message index.
    pub fn new(store_path: &str, topic: &str, queue_id: u32, max_file_size: u64) -> Result<Self> {
        let base_dir = PathBuf::from(store_path);
        let msg_index_dir = base_dir.join(topic).join(queue_id.to_string());

        fs::create_dir_all(&msg_index_dir).context(StdIOSnafu)?;

        let read_dir = msg_index_dir.read_dir().unwrap();
        let file_num = read_dir.count();

        let mut mapped_file_queue = MappedFileQueue::new(
            msg_index_dir.as_path().to_str().unwrap(), max_file_size).unwrap();

        if file_num != 0 {
            mapped_file_queue.recovery(|mmap: &MmapMut, offset: usize| {
                // TODO size of struct?
                if offset + MSG_INDEX_UNIT_SIZE < mmap.len() {
                    let mut buffer: Vec<u8> = vec![0; MSG_INDEX_UNIT_SIZE - 8];
                    buffer.copy_from_slice(&mmap[offset + 8..offset + MSG_INDEX_UNIT_SIZE]);
                    let msg_unit_slice = buffer.as_slice();

                    let size_bytes: [u8; 4] = msg_unit_slice[0..4].try_into().unwrap();
                    let size = u32::from_le_bytes(size_bytes);

                    if size > 0 {
                        return Some(offset + MSG_INDEX_UNIT_SIZE);
                    }
                }
                None
            });
        }

        Ok(MessageIndex { mapped_file_queue })
    }

    // Write data to the memory-mapped file.
    pub fn put_msg_index(&mut self, msg_offset: usize, msg_size: usize) -> Result<usize> {
        println!("put_msg_index: msg_offset={} msg_size={}", msg_offset, msg_size);

        // Convert u64 and u32 values to the byte arrays
        let offset_bytes = u64::to_le_bytes(msg_offset as u64);
        let size_bytes = u32::to_le_bytes(msg_size as u32);

        let mut index_unit_bytes: Vec<u8> = Vec::new();
        index_unit_bytes.extend_from_slice(offset_bytes.as_slice());
        index_unit_bytes.extend_from_slice(size_bytes.as_slice());

        self.mapped_file_queue.append(&index_unit_bytes)
    }

    pub fn read_msg_index(&self, index_offset: usize) -> Result<MessageIndexUnit> {
        let offset = index_offset * MSG_INDEX_UNIT_SIZE;

        let msg_unit_bytes = self.mapped_file_queue.read(offset, MSG_INDEX_UNIT_SIZE)?;
        let msg_unit_slice = msg_unit_bytes.as_slice();

        // Read the values from the array at the specified positions
        let offset_bytes: [u8; 8] = msg_unit_slice[0..8].try_into().unwrap();
        let size_bytes: [u8; 4] = msg_unit_slice[8..12].try_into().unwrap();

        // Convert the byte arrays to u64 and u32 values
        let offset = u64::from_le_bytes(offset_bytes);
        let size = u32::from_le_bytes(size_bytes);

        // Create and return a MessageIndexUnit object
        Ok(MessageIndexUnit { offset, size })
    }
}