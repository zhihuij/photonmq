use std::io;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::index::MessageIndexFile;
use crate::mmap_file::MemoryMappedFile;

pub struct CommitLog {
    current_file: Arc<RwLock<MemoryMappedFile>>,
    current_index_file: Arc<RwLock<MessageIndexFile>>,
    index_counter: AtomicUsize,
}

impl CommitLog {
    pub fn new(base_path: &str, max_file_size: u64) -> io::Result<Self> {
        let base_dir = PathBuf::from(base_path);
        let msg_log_path = base_dir.join("test.log");
        let msg_index_path = base_dir.join("test.index");
        // Create or open the initial MemoryMappedFile
        let current_file = Arc::new(RwLock::new(
            MemoryMappedFile::open(msg_log_path.to_str().unwrap(), max_file_size)?));
        let current_index_file = Arc::new(RwLock::new(
            MessageIndexFile::open(msg_index_path.to_str().unwrap(), max_file_size)?));

        Ok(CommitLog {
            current_file,
            current_index_file,
            index_counter: AtomicUsize::new(0),
        })
    }

    fn increment_index(&self) -> usize {
        self.index_counter.fetch_add(1, Ordering::SeqCst)
    }

    pub async fn write_records(&self, data: Vec<u8>) -> io::Result<()> {
        let mut current_file = self.current_file.write().unwrap();
        let mut current_index_file = self.current_index_file.write().unwrap();
        // *current_file = MemoryMappedFile::open(base_path_clone.as_str(), max_file_size).unwrap();

        // Write the record to the current file
        let offset = current_file.append(&data).unwrap();

        let msg_index = self.index_counter.load(Ordering::SeqCst);

        current_index_file.put_msg_index(
            msg_index, offset as u64, data.len() as u32).unwrap();

        self.increment_index();

        Ok(())
    }

    pub async fn read_records(&self, msg_index: usize) -> io::Result<Vec<u8>> {
        let current_index_file = self.current_index_file.read().unwrap();
        let msg_index_unit = current_index_file.read_msg_index(msg_index).unwrap();

        if msg_index_unit.size > 0 {
            // Lock the RwLock for reading
            let current_file = self.current_file.read().unwrap();
            // Read and return records
            let records = current_file.read(msg_index_unit.offset as usize,
                                            msg_index_unit.size as usize).unwrap();

            Ok(records)
        } else {
            Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Message size is zero",
            ))
        }
    }
}