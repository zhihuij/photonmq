use std::fs::{OpenOptions};
use std::io::{self, Result};
use memmap2::{MmapMut};

pub struct MessageIndexFile {
    mmap: MmapMut,
    start_index: usize,
}

const MSG_INDEX_UNIT_SIZE: usize = std::mem::size_of::<MessageIndexUnit>();

pub struct MessageIndexUnit {
    pub offset: u64,
    pub size: u32,
}

impl MessageIndexFile {
    // Constructor: Open or create a file for message index.
    pub fn open(file_path: &str, file_size: u64) -> Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(file_path)?;
        file.set_len(file_size)?;

        let mmap = unsafe { MmapMut::map_mut(&file)? };

        let start_index: usize = 0;

        Ok(MessageIndexFile { mmap, start_index })
    }

    // Write data to the memory-mapped file.
    pub fn put_msg_index(&mut self, msg_index: usize, msg_offset: u64, msg_size: u32) -> Result<usize> {
        // Ensure the msg_index matches the content of the index.
        if (msg_index - self.start_index) * MSG_INDEX_UNIT_SIZE < self.mmap.len() {
            // Define the positions in the array to read the values
            let offset_position = msg_index * MSG_INDEX_UNIT_SIZE;
            let size_position = offset_position + 8;

            // Convert u64 and u32 values to the byte arrays
            let offset_bytes = u64::to_le_bytes(msg_offset);
            let size_bytes = u32::to_le_bytes(msg_size);

            // Write the values from the array at the specified positions
            self.mmap[offset_position..offset_position + 8].copy_from_slice(offset_bytes.as_slice());
            self.mmap[size_position..size_position + 4].copy_from_slice(size_bytes.as_slice());

            self.mmap.flush()?;

            println!("produce message: msg_index={} msg_offset={} msg_size={}",
                     msg_index, msg_offset, msg_size);

            Ok(msg_index)
        } else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Data size exceeds the mapped region's size.",
            ));
        }
    }

    pub fn read_msg_index(&self, msg_index: usize) -> Result<MessageIndexUnit> {
        // Ensure the msg_index matches the content of the index.
        if (msg_index - self.start_index) * MSG_INDEX_UNIT_SIZE < self.mmap.len() {
            // Define the positions in the array to read the values
            let offset_position = msg_index * MSG_INDEX_UNIT_SIZE;
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
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Buffer size doesn't match the mapped region's size.",
            ));
        }
    }
}