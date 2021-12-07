use std::fmt;



#[derive(Debug)]
pub enum ConvertError {
    NotSupported(String, String),
}

impl fmt::Display for ConvertError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Couldn't convert the row `{:?}` to a desired type",
            self.to_owned()
        )
    }
}

impl From<serde_json::Error> for ConvertError {
    fn from(err: serde_json::Error) -> Self {
        ConvertError::NotSupported(err.to_string(), "SerdeJson".to_string())
    }
}

impl From<serde_json::Error> for AkitaDataError {
    fn from(err: serde_json::Error) -> Self {
        AkitaDataError::ConvertError(ConvertError::NotSupported(err.to_string(), "SerdeJson".to_string()))
    }
}



#[derive(Debug)]
pub enum AkitaDataError {
    ConvertError(ConvertError),
    NoSuchValueError(String),
    ObjectValidError(String),
}