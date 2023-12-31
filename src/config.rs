use config::{Config, Environment, File, FileFormat};
use secrecy::SecretString;
use serde::{Deserialize, Serialize};
use snafu::ResultExt;
use crate::error::{LoadLayeredConfigSnafu, Result};
use crate::storage::msg_index::MSG_INDEX_UNIT_SIZE;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct ConfigOptions {
    pub msg_store_path: String,
    pub topic_store_path: String,
    pub index_file_size: u64,
    pub msg_store_file_size: u64,
    pub storage: StorageConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct StorageConfig {
    #[serde(flatten)]
    pub store: ObjectStoreConfig,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            store: ObjectStoreConfig::default(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum ObjectStoreConfig {
    S3(S3Config),
    Oss(OssConfig),
}

impl Default for ObjectStoreConfig {
    fn default() -> Self {
        ObjectStoreConfig::Oss(OssConfig::default())
    }
}

const DEFAULT_INDEX_FILE_SIZE: u64 = 300000 * MSG_INDEX_UNIT_SIZE as u64;
const DEFAULT_MSG_STORE_FILE_SIZE: u64 = 1024 * 1024 * 1024;

impl Default for ConfigOptions {
    fn default() -> Self {
        Self {
            msg_store_path: String::default(),
            topic_store_path: String::default(),
            index_file_size: DEFAULT_INDEX_FILE_SIZE,
            msg_store_file_size: DEFAULT_MSG_STORE_FILE_SIZE,
            storage: StorageConfig::default(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct OssConfig {
    pub bucket: String,
    #[serde(skip_serializing)]
    pub access_key_id: SecretString,
    #[serde(skip_serializing)]
    pub access_key_secret: SecretString,
    pub endpoint: String,
}

impl Default for OssConfig {
    fn default() -> Self {
        Self {
            bucket: String::default(),
            access_key_id: SecretString::from(String::default()),
            access_key_secret: SecretString::from(String::default()),
            endpoint: String::default(),
        }
    }
}


#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct S3Config {
    pub bucket: String,
    #[serde(skip_serializing)]
    pub access_key_id: SecretString,
    #[serde(skip_serializing)]
    pub access_key_secret: SecretString,
    pub endpoint: String,
    pub region: Option<String>,
}

impl Default for S3Config {
    fn default() -> Self {
        Self {
            bucket: String::default(),
            access_key_id: SecretString::from(String::default()),
            access_key_secret: SecretString::from(String::default()),
            endpoint: String::default(),
            region: Option::default(),
        }
    }
}

impl ConfigOptions {
    pub fn load_layered_options<'de, T: Serialize + Deserialize<'de> + Default>() -> Result<T> {
        let default_opts = T::default();

        // Add default values as the sources of the configuration
        let mut layered_config = Config::builder()
            .add_source(Config::try_from(&default_opts).context(LoadLayeredConfigSnafu)?)
            .add_source(Environment::with_prefix("PN")
                .try_parsing(true)
                .separator("__"));

        // Add config file as the source of the configuration
        layered_config = layered_config.add_source(File::new("./config.toml", FileFormat::Toml));


        let opts = layered_config
            .build()
            .context(LoadLayeredConfigSnafu)?
            .try_deserialize()
            .context(LoadLayeredConfigSnafu)?;

        Ok(opts)
    }
}

