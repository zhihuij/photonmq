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

    #[snafu(display("Failed to encode message to binary format"))]
    EncodeMsgJson {
        location: Location,
        source: serde_json::Error,
    },

    #[snafu(display("Failed to decode binary to message"))]
    DecodeMsgJson {
        location: Location,
        source: serde_json::Error,
    },

    #[snafu(display("Failed to decode binary to message"))]
    StdIO {
        location: Location,
        source: std::io::Error,
    },

    #[snafu(display("Failed to decode binary to message"))]
    InvalidInput {
        location: Location,
        msg: String
    },
}

pub type Result<T> = std::result::Result<T, Error>;