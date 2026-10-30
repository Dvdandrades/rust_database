use crate::error::{DbError, Result};
use serde::{Deserialize, Serialize};
use wincode::{SchemaRead, SchemaWrite};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecordType {
    Put = 1,
    Delete = 2,
    BatchBegin = 3,
}

impl TryFrom<u8> for RecordType {
    type Error = DbError;

    fn try_from(value: u8) -> Result<Self> {
        match value {
            1 => Ok(RecordType::Put),
            2 => Ok(RecordType::Delete),
            3 => Ok(RecordType::BatchBegin),
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BatchOp {
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WriteBatch {
    ops: Vec<BatchOp>,
}

impl WriteBatch {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            ops: Vec::with_capacity(capacity),
        }
    }

    pub fn put(&mut self, key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) -> &mut Self {
        self.ops.push(BatchOp::Put {
            key: key.into(),
            value: value.into(),
        });
        self
    }

    pub fn delete(&mut self, key: impl Into<Vec<u8>>) -> &mut Self {
        self.ops.push(BatchOp::Delete { key: key.into() });
        self
    }

    pub fn len(&self) -> usize {
        self.ops.len()
    }

    pub fn is_empty(&self) -> bool {
        self.ops.is_empty()
    }

    pub fn ops(&self) -> &[BatchOp] {
        &self.ops
    }

    pub(crate) fn into_ops(self) -> Vec<BatchOp> {
        self.ops
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_write_batch_builder() {
        let mut batch = WriteBatch::new();
        assert!(batch.is_empty());

        batch
            .put("a", "1")
            .put(b"b".to_vec(), b"2".to_vec())
            .delete("c");

        assert_eq!(batch.len(), 3);
        assert_eq!(
            batch.ops(),
            &[
                BatchOp::Put {
                    key: b"a".to_vec(),
                    value: b"1".to_vec()
                },
                BatchOp::Put {
                    key: b"b".to_vec(),
                    value: b"2".to_vec()
                },
                BatchOp::Delete { key: b"c".to_vec() },
            ]
        );
    }

    #[test]
    fn test_batch_begin_record_type() {
        assert_eq!(RecordType::try_from(3).unwrap(), RecordType::BatchBegin);
        assert!(RecordType::try_from(4).is_err());
    }

    #[test]
    fn test_record_serialization() {
        let record = Record::new_put(b"my_key".to_vec(), b"my_value".to_vec());
        let serialized = record.serialize().unwrap();

        let deserialized = Record::deserialize(&serialized).unwrap();
        assert_eq!(record, deserialized);
    }
}
