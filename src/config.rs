use config::{Config, ConfigError, File, FileFormat};
use serde::{Deserialize, Serialize};
use snafu::{Location, ResultExt, Snafu};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct ConfigOptions {
    pub msg_store_path: Option<String>,
    pub topic_store_path: Option<String>,
}

impl Default for ConfigOptions {
    fn default() -> Self {
        Self {
            msg_store_path: None,
            topic_store_path: None,
        }
    }
}

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Failed to load layered config"))]
    LoadLayeredConfig {
        source: ConfigError,
        location: Location,
    },
}

pub type Result<T> = std::result::Result<T, Error>;

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

