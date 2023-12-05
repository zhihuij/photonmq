use std::str::FromStr;
use bytes::{BufMut, BytesMut};
use memmap2::MmapMut;
use crate::error::{ObjectStoreAccessSnafu, ObjectStoreBuildSnafu, Result};
use opendal::Operator;
use opendal::services::Oss;
use secrecy::{ExposeSecret};
use snafu::{location, Location, ResultExt};
use crate::config::OssConfig;
use crate::error::Error::{InvalidInput};

pub struct OssObjectStoreFile {
    file_path: String,
    object_store: Operator,
    min_offset: usize,
    max_offset: usize,
    max_file_size: usize,
    write_cache: BytesMut,
    read_cache: BytesMut,
}

pub const OBJECT_STORE_FILE_SIZE: usize = 1024 * 1024;

impl OssObjectStoreFile {
    pub fn new(oss_config: OssConfig, file_path: &str, start_offset: usize, file_size: u64) -> Result<Self> {
        let mut builder = Oss::default();
        let _ = builder
            .bucket(&oss_config.bucket)
            .endpoint(&oss_config.endpoint)
            .access_key_id(oss_config.access_key_id.expose_secret())
            .access_key_secret(oss_config.access_key_secret.expose_secret());

        let object_store = Operator::new(builder)
            .context(ObjectStoreBuildSnafu)?
            .finish();

        Ok(OssObjectStoreFile {
            file_path: file_path.to_string(),
            object_store,
            min_offset: start_offset,
            max_offset: start_offset,
            max_file_size: file_size as usize,
            write_cache: BytesMut::with_capacity(file_size as usize),
            read_cache: BytesMut::with_capacity(file_size as usize),
        })
    }

    pub fn get_min_offset(&self) -> usize {
        self.min_offset
    }

    pub fn get_max_offset(&self) -> usize {
        self.max_offset
    }

    pub fn read_record<Func>(&mut self, _reader: &Func)
        where Func: Fn(&MmapMut, usize) -> Option<usize> {
        // let mut write_pos = 0;
        // loop {
        //     let read_result = reader(&self.mmap, write_pos);
        //     match read_result {
        //         None => { break; }
        //         Some(offset) => {
        //             write_pos += offset;
        //             self.max_offset += offset;
        //         }
        //     }
        // }
    }

    pub async fn append(&mut self, data: &Vec<u8>) -> Result<usize> {
        let data_len = data.len();
        let write_pos = self.max_offset - self.min_offset;

        if write_pos + data_len > self.max_file_size {
            // write cache data to remote file
            let cache_data = self.write_cache.split();
            self.object_store.write(self.file_path.as_str(), cache_data).await.context(ObjectStoreAccessSnafu)?;
        }

        self.write_cache.put_slice(data.as_slice());

        let old_offset = self.max_offset;
        self.max_offset += data_len;

        Ok(old_offset)
    }

    pub async fn read(&mut self, offset: usize, data_size: usize) -> Result<Vec<u8>> {
        let mut buffer = vec![0; data_size];
        let read_pos = offset - self.min_offset;
        if self.read_cache.len() == 0 {
            // read data from remote file
            let read_data = self.object_store.read(self.file_path.as_str()).await.context(ObjectStoreAccessSnafu)?;
            self.read_cache.put_slice(read_data.as_slice());
            println!("read_cache_size: {}", self.read_cache.len());
        }

        // Ensure read offset + data size doesn't exceed the max offset.
        if read_pos + data_size <= self.max_offset {
            buffer.copy_from_slice(&self.read_cache[read_pos..read_pos + data_size]);

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
    use dotenv::dotenv;
    use crate::config::{ConfigOptions, ObjectStoreConfig};
    use crate::error::Result;
    use crate::storage::object_store::OssObjectStoreFile;

    #[tokio::test]
    pub async fn test_write_read() -> Result<()> {
        let file_size = 32;
        let file_path = "test.log";
        dotenv().ok();
        let config_options: ConfigOptions = ConfigOptions::load_layered_options().unwrap();
        println!("loaded config: {:?}", config_options);

        let oss_config = match config_options.storage.store {
            ObjectStoreConfig::Oss(cfg) => {
                Some(cfg)
            }
            _ => { None }
        };

        let mut object_file = OssObjectStoreFile::new(oss_config.unwrap(),
                                                      file_path, 0, file_size)?;

        let data_to_write = "Hello, Memory-Mapped File!";
        for i in 0..2 {
            // Write data to the memory-mapped file.
            let str = format!("{}{}", data_to_write, i);
            object_file.append(str.as_bytes().to_vec().as_ref()).await?;
        }

        let data_size = data_to_write.len() + 1;

        // Read data from the memory-mapped file.
        let read_buffer = object_file.read(0, data_size).await?;

        // Display the read data.
        let read_data = String::from_utf8(read_buffer);
        println!("{}", read_data.unwrap().as_str());


        Ok(())
    }
}