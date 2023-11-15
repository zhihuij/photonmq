use std::fs::OpenOptions;
use memmap2::{MmapMut};
use snafu::{location, Location, ResultExt};
use crate::error::{Result, StdIOSnafu};
use crate::error::Error::InvalidInput;

pub struct MemoryMappedFile {
    mmap: MmapMut,
    min_offset: usize,
    max_offset: usize,
}

impl MemoryMappedFile {
    // Constructor: Open or create a memory-mapped file.
    pub fn new(file_path: &str, start_offset: usize, file_size: u64) -> Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(file_path).context(StdIOSnafu)?;
        file.set_len(file_size).context(StdIOSnafu)?;

        let mmap = unsafe { MmapMut::map_mut(&file).context(StdIOSnafu)? };

        // TODO max_offset?
        Ok(MemoryMappedFile { mmap, min_offset: start_offset, max_offset: start_offset })
    }

    pub fn get_min_offset(&self) -> usize {
        self.min_offset
    }

    pub fn get_max_offset(&self) -> usize {
        self.max_offset
    }

    pub fn set_max_offset(&mut self, max_offset: usize) {
        self.max_offset = max_offset;
    }

    pub fn read_record<Func>(&mut self, reader: &Func)
        where Func: Fn(&MmapMut, usize) -> Option<usize> {
        let mut write_pos = 0;
        loop {
            let read_result = reader(&self.mmap, write_pos);
            match read_result {
                None => { break; }
                Some(offset) => {
                    write_pos += offset;
                    self.max_offset += offset;
                }
            }
        }
    }

    // Write data to the memory-mapped file.
    pub fn append(&mut self, data: &Vec<u8>) -> Result<usize> {
        let data_len = data.len();
        let write_pos = self.max_offset - self.min_offset;

        // Ensure the data fits within the mapped region.
        if write_pos + data_len <= self.mmap.len() {
            self.mmap[write_pos..write_pos + data_len].copy_from_slice(data.as_slice());

            // Flush changes to disk (optional).
            self.mmap.flush().context(StdIOSnafu)?;

            let old_offset = self.max_offset;
            self.max_offset += data_len;

            Ok(old_offset)
        } else {
            Err(InvalidInput {
                location: location!(),
                msg: "Buffer size doesn't match the mapped region's size.".to_string(),
            })
        }
    }

    // Read data from the memory-mapped file.
    pub fn read(&self, offset: usize, data_size: usize) -> Result<Vec<u8>> {
        let mut buffer = vec![0; data_size];
        let read_pos = offset - self.min_offset;

        // TODO mmap.len should be max_offset?
        // Ensure the buffer size matches the mapped region.
        if read_pos + data_size < self.mmap.len() {
            buffer.copy_from_slice(&self.mmap[read_pos..read_pos + data_size]);

            Ok(buffer)
        } else {
            Err(InvalidInput {
                location: location!(),
                msg: "Buffer size doesn't match the mapped region's size.".to_string(),
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use tempfile::{TempDir};
    use crate::mmap_file::MemoryMappedFile;
    use crate::error::Result;

    pub fn create_temp_dir(prefix: &str) -> TempDir {
        tempfile::Builder::new().prefix(prefix).tempdir().unwrap()
    }

    #[tokio::test]
    pub async fn test_write_read() -> Result<()> {
        let dir_path = create_temp_dir("mmap_test");
        let file_size = 1024;
        let file_path = dir_path.path().join("temp_mmap_file");
        // Create or open the memory-mapped file.
        let mut mem_mapped_file = MemoryMappedFile::new(
            file_path.to_str().unwrap(), 0, file_size)?;

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
