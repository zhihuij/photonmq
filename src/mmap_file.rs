use std::fs::{OpenOptions};
use std::io::{self, Result};
use memmap2::{MmapMut};

pub struct MemoryMappedFile {
    mmap: MmapMut,
    offset: usize,
}

impl MemoryMappedFile {
    // Constructor: Open or create a memory-mapped file.
    pub fn open(file_path: &str, file_size: u64) -> Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(file_path)?;
        file.set_len(file_size)?;

        let mmap = unsafe { MmapMut::map_mut(&file)? };

        let offset: usize = 0;

        Ok(MemoryMappedFile { mmap, offset })
    }

    // Write data to the memory-mapped file.
    pub fn append(&mut self, data: &Vec<u8>) -> Result<usize> {
        let data_len = data.len();

        // Ensure the data fits within the mapped region.
        if self.offset + data_len <= self.mmap.len() {
            self.mmap[self.offset..self.offset + data_len].copy_from_slice(data.as_slice());

            // Flush changes to disk (optional).
            self.mmap.flush()?;

            let old_offset = self.offset;
            self.offset += data_len;

            Ok(old_offset)
        } else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Data size exceeds the mapped region's size.",
            ));
        }
    }

    // Read data from the memory-mapped file.
    pub fn read(&self, offset: usize, data_size: usize) -> Result<Vec<u8>> {
        let mut buffer = vec![0; data_size];

        // Ensure the buffer size matches the mapped region.
        if offset + data_size < self.mmap.len() {
            buffer.copy_from_slice(&self.mmap[offset..offset + data_size]);

            Ok(buffer)
        } else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Buffer size doesn't match the mapped region's size.",
            ));
        }
    }
}

#[cfg(test)]
mod tests {
    use tempfile::{TempDir};
    use crate::mmap_file::MemoryMappedFile;
    use std::io::{Result};

    pub fn create_temp_dir(prefix: &str) -> TempDir {
        tempfile::Builder::new().prefix(prefix).tempdir().unwrap()
    }

    #[tokio::test]
    pub async fn test_write_read() -> Result<()> {
        let dir_path = create_temp_dir("mmap_test");
        let file_size = 1024;
        let file_path = dir_path.path().join("temp_mmap_file");
        // Create or open the memory-mapped file.
        let mut mem_mapped_file = MemoryMappedFile::open(file_path.to_str().unwrap(), file_size)?;

        // Write data to the memory-mapped file.
        let data_to_write = "Hello, Memory-Mapped File!";
        mem_mapped_file.append(data_to_write.as_bytes().to_vec().as_ref())?;

        // Read data from the memory-mapped file.
        let read_buffer = mem_mapped_file.read(0, data_to_write.len())?;

        // Display the read data.
        let read_data = String::from_utf8(read_buffer);
        assert_eq!(data_to_write, read_data.unwrap().as_str());

        Ok(())
    }
}
