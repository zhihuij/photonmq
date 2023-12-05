use snafu::{Location, Snafu};

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Failed to load layered config"))]
    LoadLayeredConfig {
        location: Location,
        source: config::ConfigError,
    },

    #[snafu(display("Failed to encode message to binary format"))]
    EncodeMsgBin {
        location: Location,
        source: bincode::Error,
    },

    #[snafu(display("Failed to decode binary to message"))]
    DecodeMsgBin {
        location: Location,
        source: bincode::Error,
    },

    #[snafu(display("Failed to decode binary to message"))]
    StdIO {
        location: Location,
        source: std::io::Error,
    },

    #[snafu(display("Failed to execute sql"))]
    Rusqlite {
        location: Location,
        source: rusqlite::Error,
    },

    #[snafu(display("Failed to decode binary to message"))]
    InvalidInput {
        location: Location,
        msg: String,
    },

    #[snafu(display("Failed to build the object store"))]
    ObjectStoreBuild {
        location: Location,
        source: opendal::Error
    },

    #[snafu(display("Failed to access the object store"))]
    ObjectStoreAccess {
        location: Location,
        source: opendal::Error
    },
}

pub type Result<T> = std::result::Result<T, Error>;