use std::collections::HashMap;
use std::sync::RwLock;

#[derive(Debug, Clone, Copy)]
pub struct KeyDirEntry {
    pub file_id: u32,
    pub offset: u64,
    pub size: u32,
}

pub struct KeyIndex {
    index: RwLock<HashMap<Vec<u8>, KeyDirEntry>>,
}

impl KeyIndex {
    pub fn new() -> Self {
        Self {
            index: RwLock::new(HashMap::new()),
        }
    }

    pub fn insert(&self, key: Vec<u8>, file_id: u32, offset: u64, size: u32) {
        let mut guard = self.index.write().unwrap();
        guard.insert(
            key,
            KeyDirEntry {
                file_id,
                offset,
                size,
            },
        );
    }

    pub fn get(&self, key: &[u8]) -> Option<KeyDirEntry> {
        let guard = self.index.read().unwrap();
        guard.get(key).copied()
    }

    pub fn remove(&self, key: &[u8]) -> Option<KeyDirEntry> {
        let mut guard = self.index.write().unwrap();
        guard.remove(key)
    }

    pub fn clear(&self) {
        let mut guard = self.index.write().unwrap();
        guard.clear();
    }

    pub fn get_all(&self) -> Vec<(Vec<u8>, KeyDirEntry)> {
        let guard = self.index.read().unwrap();
        guard.iter().map(|(k, v)| (k.clone(), *v)).collect()
    }
}

impl Default for KeyIndex {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_and_default() {
        let index_new = KeyIndex::new();
        let index_default: KeyIndex = KeyIndex::default();

        assert!(index_new.get(b"test").is_none());
        assert!(index_default.get(b"test").is_none());
    }

    #[test]
    fn test_insert_and_get() {
        let index = KeyIndex::new();
        let key = b"my_key".to_vec();

        index.insert(key.clone(), 1, 100, 42);

        let entry = index.get(&key);
        assert!(entry.is_some());

        let entry = entry.unwrap();
        assert_eq!(entry.file_id, 1);
        assert_eq!(entry.offset, 100);
        assert_eq!(entry.size, 42);
    }

    #[test]
    fn test_remove() {
        let index = KeyIndex::new();
        let key = b"removable_key".to_vec();

        index.insert(key.clone(), 1, 50, 10);
        assert!(index.get(&key).is_some());

        let removed = index.remove(&key);
        assert!(removed.is_some());

        let entry = removed.unwrap();
        assert_eq!(entry.file_id, 1);
        assert_eq!(entry.offset, 50);
        assert_eq!(entry.size, 10);

        assert!(index.get(&key).is_none());
        assert!(index.remove(&key).is_none());
    }

    #[test]
    fn test_clear() {
        let index = KeyIndex::new();
        index.insert(b"key1".to_vec(), 1, 10, 5);
        index.insert(b"key2".to_vec(), 1, 20, 10);

        assert!(index.get(b"key1").is_some());
        assert!(index.get(b"key2").is_some());

        index.clear();

        assert!(index.get(b"key1").is_none());
        assert!(index.get(b"key2").is_none());
    }
}
