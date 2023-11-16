use std::path::{Path, PathBuf};
use memmap2::MmapMut;
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
    pub fn new(store_path: &str, max_file_size: u64) -> Result<Self> {
        Ok(MappedFileQueue { store_path: store_path.to_string(), max_file_size, mapped_files: Vec::new() })
    }

    /*
     * Recovery from restart or fault, load existed files.
     */
    pub fn recovery<Func>(&mut self, reader: Func)
        where Func: Fn(&MmapMut, usize) -> Option<usize> {
        let store_path_dir = Path::new(&self.store_path);
        // TODO only read the several newest file
        for entry in store_path_dir.read_dir().unwrap() {
            if let Ok(entry) = entry {
                let entry_path = entry.path();
                if entry_path.is_file() {
                    let mapped_file_name = entry_path.file_name().unwrap().to_str().unwrap();
                    let mapped_file_path = entry_path.to_str().unwrap();
                    let start_offset: usize = mapped_file_name.parse().expect("Error while parse file name");
                    let mut mapped_file = MemoryMappedFile::new(
                        mapped_file_path, start_offset, self.max_file_size).expect("Error while load mapped file");

                    mapped_file.read_record(&reader);

                    println!("loaded mapped file: {:?}, offset={}, max_offset={}", &entry.path(),
                             mapped_file.get_min_offset(), mapped_file.get_max_offset());
                    self.mapped_files.push(mapped_file);
                }
            }
        }
    }

    pub fn create_mapped_file(&mut self, start_offset: usize) -> &mut MemoryMappedFile {
        let store_path_clone = self.store_path.clone();
        let base_dir = PathBuf::from(store_path_clone);
        let file_name = format!("{:020}", start_offset);
        let file_path = base_dir.join(file_name);

        println!("new memory mapped file: {:?}", &file_path);

        let mapped_file = MemoryMappedFile::new(
            file_path.as_path().to_str().unwrap(), start_offset, self.max_file_size).unwrap();

        self.mapped_files.push(mapped_file);
        self.mapped_files.last_mut().unwrap()
    }

    fn get_last_mapped_file_mut(&mut self) -> &mut MemoryMappedFile {
        if self.mapped_files.len() == 0 {
            self.create_mapped_file(0);
        }
        self.mapped_files.last_mut().unwrap()
    }

    pub fn append(&mut self, data: &Vec<u8>) -> Result<usize> {
        let mapped_file = self.get_last_mapped_file_mut();
        let append_result = mapped_file.append(data);

        match append_result {
            Ok(write_offset) => { Ok(write_offset) }
            Err(err) => {
                match err {
                    InvalidInput { .. } => {
                        // data size exceed the size of current file, create a new one and retry
                        let max_offset = mapped_file.get_max_offset();
                        let new_mapped_file = self.create_mapped_file(max_offset);

                        new_mapped_file.append(data)
                    }
                    other => {
                        Err(other)
                    }
                }
            }
        }
    }

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

#[cfg(test)]
mod tests {
    use tempfile::{TempDir};
    use crate::error::Result;
    use crate::mapped_file_queue::MappedFileQueue;

    pub fn create_temp_dir(prefix: &str) -> TempDir {
        tempfile::Builder::new().prefix(prefix).tempdir().unwrap()
    }

    #[tokio::test]
    pub async fn test_write_read() -> Result<()> {
        let dir_path = create_temp_dir("topic_mgr_test");
        // Create or open the memory-mapped file.
        let mut mapped_file_queue = MappedFileQueue::new(
            dir_path.path().to_str().unwrap(), 20)?;

        let test_str = "hello world".as_bytes();
        let test_data = Vec::from(test_str);

        mapped_file_queue.append(&test_data).expect("Error while write");
        mapped_file_queue.append(&test_data).expect("Error while write");

        assert_eq!(mapped_file_queue.get_mapped_files().len(), 2);

        Ok(())
    }
}
