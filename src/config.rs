use config::{Config, File, FileFormat};
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
}

const DEFAULT_INDEX_FILE_SIZE: u64 = 300000 * MSG_INDEX_UNIT_SIZE as u64;
const DEFAULT_MSG_STORE_FILE_SIZE: u64 = 1024 * 1024 * 1024;

impl Default for ConfigOptions {
    fn default() -> Self {
        Self {
            msg_store_path: "".to_string(),
            topic_store_path: "".to_string(),
            index_file_size: DEFAULT_INDEX_FILE_SIZE,
            msg_store_file_size: DEFAULT_MSG_STORE_FILE_SIZE,
        }
    }
}

impl ConfigOptions {
    pub fn load_layered_options<'de, T: Serialize + Deserialize<'de> + Default>() -> Result<T> {
        let default_opts = T::default();

        // Add default values as the sources of the configuration
        let mut layered_config = Config::builder()
            .add_source(Config::try_from(&default_opts).context(LoadLayeredConfigSnafu)?);

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

