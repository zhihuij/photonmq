use std::fs::{OpenOptions};
use std::path::PathBuf;
use std::{fs, u32, u64, usize};
use memmap2::{MmapMut};
use snafu::{location, Location, ResultExt};
use crate::error::{Result, StdIOSnafu};
use crate::error::Error::InvalidInput;

pub struct MessageIndex {
    mmap: MmapMut,
    start_index: u64,
}

pub const MSG_INDEX_UNIT_SIZE: usize = std::mem::size_of::<MessageIndexUnit>();

pub struct MessageIndexUnit {
    pub offset: u64,
    pub size: u32,
}

impl MessageIndex {
    // Constructor: Open or create a file for message index.
    pub fn open(store_path: &str, topic: &str, queue_id: u32, file_size: u64) -> Result<Self> {
        let base_dir = PathBuf::from(store_path);
        let msg_index_dir = base_dir.join(topic).join(queue_id.to_string());

        fs::create_dir_all(&msg_index_dir).context(StdIOSnafu)?;
        let msg_index_file = msg_index_dir.join("test.index");
        println!("index file path: {:?}", &msg_index_file);

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(msg_index_file).context(StdIOSnafu)?;
        file.set_len(file_size).context(StdIOSnafu)?;

        let mmap = unsafe { MmapMut::map_mut(&file).context(StdIOSnafu)? };

        let start_index: u64 = 0;

        Ok(MessageIndex { mmap, start_index })
    }

    // Write data to the memory-mapped file.
    pub fn put_msg_index(&mut self, index_offset: u64, msg_offset: usize, msg_size: usize) -> Result<u64> {
        // Ensure the msg_index matches the content of the index.
        if (index_offset - self.start_index) * (MSG_INDEX_UNIT_SIZE as u64) < self.mmap.len() as u64 {
            // Define the positions in the array to read the values
            let offset_position = index_offset as usize * MSG_INDEX_UNIT_SIZE;
            let size_position = offset_position + 8;

            // Convert u64 and u32 values to the byte arrays
            let offset_bytes = u64::to_le_bytes(msg_offset as u64);
            let size_bytes = u32::to_le_bytes(msg_size as u32);

            // Write the values from the array at the specified positions
            self.mmap[offset_position..offset_position + 8].copy_from_slice(offset_bytes.as_slice());
            self.mmap[size_position..size_position + 4].copy_from_slice(size_bytes.as_slice());

            self.mmap.flush().context(StdIOSnafu)?;

            println!("produce message: msg_index={} msg_offset={} msg_size={}",
                     index_offset, msg_offset, msg_size);

            Ok(index_offset)
        } else {
            Err(InvalidInput {
                location: location!(),
                msg: "Buffer size doesn't match the mapped region's size.".to_string(),
            })
        }
    }

    pub fn read_msg_index(&self, index_offset: u64) -> Result<MessageIndexUnit> {
        // Ensure the msg_index matches the content of the index.
        if (index_offset - self.start_index) * (MSG_INDEX_UNIT_SIZE as u64) < self.mmap.len() as u64 {
            // Define the positions in the array to read the values
            let offset_position = index_offset as usize * MSG_INDEX_UNIT_SIZE;
            let size_position = offset_position + 8;

            // Read the values from the array at the specified positions
            let offset_bytes: [u8; 8] = self.mmap[offset_position..offset_position + 8].try_into().unwrap();
            let size_bytes: [u8; 4] = self.mmap[size_position..size_position + 4].try_into().unwrap();

            // Convert the byte arrays to u64 and u32 values
            let offset = u64::from_le_bytes(offset_bytes);
            let size = u32::from_le_bytes(size_bytes);

            // Create and return a MessageIndexUnit object
            Ok(MessageIndexUnit { offset, size })
        } else {
            Err(InvalidInput {
                location: location!(),
                msg: "Buffer size doesn't match the mapped region's size.".to_string(),
            })
        }
    }
}