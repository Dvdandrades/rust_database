use crate::error::{DbError, Result};
use serde::{Deserialize, Serialize};
use wincode::{SchemaRead, SchemaWrite};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecordType {
    Put = 1,
    Delete = 2,
}

impl TryFrom<u8> for RecordType {
    type Error = DbError;

    fn try_from(value: u8) -> Result<Self> {
        match value {
            1 => Ok(RecordType::Put),
            2 => Ok(RecordType::Delete),
            other => Err(DbError::Corruption(format!(
                "Unknown record type byte: {}",
                other
            ))),
        }
    }
}

#[derive(Serialize, Deserialize, SchemaWrite, SchemaRead, Debug, Clone, PartialEq, Eq)]
pub struct Record {
    pub recid: u8,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

impl Record {
    pub fn new_put(key: Vec<u8>, value: Vec<u8>) -> Self {
        Self {
            recid: RecordType::Put as u8,
            key,
            value,
        }
    }

    pub fn new_delete(key: Vec<u8>) -> Self {
        Self {
            recid: RecordType::Delete as u8,
            key,
            value: vec![],
        }
    }

    pub fn serialize(&self) -> Result<Vec<u8>> {
        wincode::serialize(self).map_err(|e| DbError::Serialization(e.to_string()))
    }

    pub fn deserialize(bytes: &[u8]) -> Result<Self> {
        wincode::deserialize(bytes).map_err(|e| DbError::Corruption(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_record_serialization() {
        let record = Record::new_put(b"my_key".to_vec(), b"my_value".to_vec());
        let serialized = record.serialize().unwrap();

        let deserialized = Record::deserialize(&serialized).unwrap();
        assert_eq!(record, deserialized);
    }
}
