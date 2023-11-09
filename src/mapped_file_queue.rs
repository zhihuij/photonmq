use std::path::PathBuf;
use snafu::{location, Location};
use crate::error::Error::InvalidInput;
use crate::error::Result;
use crate::mmap_file::MemoryMappedFile;

pub struct MappedFileQueue {
    store_path: String,
    max_file_size: u64,
    mapped_files: Vec<MemoryMappedFile>,
}

impl MappedFileQueue {
    // Constructor: Open or create a memory-mapped file.
    pub fn open(store_path: &str, max_file_size: u64) -> Result<Self> {
        Ok(MappedFileQueue { store_path: store_path.to_string(), max_file_size, mapped_files: Vec::new() })
    }

    fn create_mapped_file(&mut self, start_offset: usize) -> Option<&mut MemoryMappedFile> {
        let store_path_clone = self.store_path.clone();
        let base_dir = PathBuf::from(store_path_clone);
        let file_name = format!("{:020}", start_offset);
        let file_path = base_dir.join(file_name);

        println!("new memory mapped file: {:?}", &file_path);

        let mapped_file = MemoryMappedFile::open(
            file_path.as_path().to_str().unwrap(), start_offset, self.max_file_size).unwrap();

        self.mapped_files.push(mapped_file);
        self.mapped_files.last_mut()
    }

    fn get_last_mapped_file_mut(&mut self, offset: usize) -> Option<&mut MemoryMappedFile> {
        if offset == 0 {
            self.create_mapped_file(offset);
        }
        self.mapped_files.last_mut()
    }

    // Write data to the memory-mapped file.
    pub fn append(&mut self, data: &Vec<u8>) -> Result<usize> {
        // TODO offset?
        let mapped_file = self.get_last_mapped_file_mut(0).unwrap();
        mapped_file.append(data)
    }

    // Read data from the memory-mapped file.
    pub fn read(&self, offset: usize, data_size: usize) -> Result<Vec<u8>> {
        let mapped_file_result = self.mapped_files.iter().find(
            |&f| f.get_min_offset() <= offset && offset < f.get_max_offset());
        match mapped_file_result {
            Some(mapped_file) => {
                mapped_file.read(offset, data_size)
            }
            None => {
                Err(InvalidInput {
                    location: location!(),
                    msg: "Invalid file offset".to_string(),
                })
            }
        }
    }
}
