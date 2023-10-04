use std::fmt;

use rocket::serde::Deserialize;

#[derive(Deserialize, Debug, PartialEq, Eq)]
pub enum TestMode {
    NoBackup,
    SerializeBackup
}

impl TestMode {
    pub fn from_string(mode: String) -> TestMode {
        match mode.as_str() {
            "SerializeBackup" => TestMode::SerializeBackup,
            _ => TestMode::NoBackup
        }
    }
}

impl fmt::Display for TestMode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            TestMode::NoBackup => write!(f, "NoBackup"),
            TestMode::SerializeBackup => write!(f, "SerializeBackup")
        }
    }
}