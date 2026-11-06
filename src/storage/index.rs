use std::collections::BTreeMap;
use std::ops::Bound;
use std::sync::RwLock;

#[derive(Debug, Clone, Copy)]
pub struct KeyDirEntry {
    pub file_id: u32,
    pub offset: u64,
    pub size: u32,
}

#[derive(Debug, Clone)]
pub enum IndexUpdate {
    Put { key: Vec<u8>, entry: KeyDirEntry },
    Remove { key: Vec<u8> },
}

fn prefix_successor(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut end = prefix.to_vec();
    while let Some(last) = end.pop() {
        if last < u8::MAX {
            end.push(last + 1);
            return Some(end);
        }
    }
    None
}

fn is_inverted(start: Bound<&[u8]>, end: Bound<&[u8]>) -> bool {
    match (start, end) {
        (Bound::Included(s) | Bound::Excluded(s), Bound::Included(e) | Bound::Excluded(e)) => {
            s > e || (s == e && matches!((start, end), (Bound::Excluded(_), Bound::Excluded(_))))
        }
        _ => false,
    }
}

pub struct KeyIndex {
    index: RwLock<BTreeMap<Vec<u8>, KeyDirEntry>>,
}

impl KeyIndex {
    pub fn new() -> Self {
        Self {
            index: RwLock::new(BTreeMap::new()),
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

    pub fn apply_batch<I>(&self, updates: I)
    where
        I: IntoIterator<Item = IndexUpdate>,
    {
        let mut guard = self.index.write().unwrap();
        for update in updates {
            match update {
                IndexUpdate::Put { key, entry } => {
                    guard.insert(key, entry);
                }
                IndexUpdate::Remove { key } => {
                    guard.remove(&key);
                }
            }
        }
    }

    pub fn clear(&self) {
        let mut guard = self.index.write().unwrap();
        guard.clear();
    }

    pub fn len(&self) -> usize {
        self.index.read().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.index.read().unwrap().is_empty()
    }

    pub fn get_all(&self) -> Vec<(Vec<u8>, KeyDirEntry)> {
        let guard = self.index.read().unwrap();
        guard.iter().map(|(k, v)| (k.clone(), *v)).collect()
    }

    pub fn range(&self, start: Bound<&[u8]>, end: Bound<&[u8]>) -> Vec<(Vec<u8>, KeyDirEntry)> {
        if is_inverted(start, end) {
            return Vec::new();
        }
        let guard = self.index.read().unwrap();
        guard
            .range::<[u8], _>((start, end))
            .map(|(k, v)| (k.clone(), *v))
            .collect()
    }

    pub fn scan_prefix(&self, prefix: &[u8]) -> Vec<(Vec<u8>, KeyDirEntry)> {
        match prefix_successor(prefix) {
            Some(end) => self.range(Bound::Included(prefix), Bound::Excluded(end.as_slice())),
            None => self.range(Bound::Included(prefix), Bound::Unbounded),
        }
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

    #[test]
    fn test_len_and_is_empty() {
        let index = KeyIndex::new();
        assert!(index.is_empty());
        assert_eq!(index.len(), 0);

        index.insert(b"a".to_vec(), 1, 0, 1);
        index.insert(b"b".to_vec(), 1, 1, 1);
        index.insert(b"a".to_vec(), 1, 2, 1);
        assert_eq!(index.len(), 2, "overwritting a key must not grow the index");
        assert!(!index.is_empty());

        index.remove(b"a");
        assert_eq!(index.len(), 1);
    }

    #[test]
    fn test_apply_batch_in_order() {
        let index = KeyIndex::new();
        index.insert(b"old".to_vec(), 1, 0, 10);
        index.insert(b"gone".to_vec(), 1, 10, 10);

        let entry = |offset| KeyDirEntry {
            file_id: 2,
            offset,
            size: 5,
        };

        index.apply_batch(vec![
            IndexUpdate::Put {
                key: b"k".to_vec(),
                entry: entry(0),
            },
            IndexUpdate::Put {
                key: b"k".to_vec(),
                entry: entry(5),
            },
            IndexUpdate::Remove {
                key: b"gone".to_vec(),
            },
            IndexUpdate::Remove {
                key: b"never_existed".to_vec(),
            },
        ]);

        assert_eq!(
            index.get(b"k").unwrap().offset,
            5,
            "the last update must win"
        );
        assert!(index.get(b"gone").is_none());
        assert_eq!(index.get(b"old").unwrap().offset, 0);
    }

    fn put(index: &KeyIndex, key: &[u8]) {
        index.insert(key.to_vec(), 1, 0, 1);
    }

    fn keys(entries: Vec<(Vec<u8>, KeyDirEntry)>) -> Vec<Vec<u8>> {
        entries.into_iter().map(|(k, _)| k).collect()
    }

    #[test]
    fn test_get_all_is_sorted() {
        let index = KeyIndex::new();
        for k in ["m", "a", "z", "b"] {
            put(&index, k.as_bytes());
        }
        assert_eq!(
            keys(index.get_all()),
            vec![b"a".to_vec(), b"b".to_vec(), b"m".to_vec(), b"z".to_vec()]
        );
    }

    #[test]
    fn test_scan_prefix() {
        let index = KeyIndex::new();
        for k in ["user:2", "user:1", "user", "users", "usf", "usd", "other"] {
            put(&index, k.as_bytes());
        }

        assert_eq!(
            keys(index.scan_prefix(b"user:")),
            vec![b"user:1".to_vec(), b"user:2".to_vec()]
        );
        assert_eq!(
            keys(index.scan_prefix(b"user")),
            vec![
                b"user".to_vec(),
                b"user:1".to_vec(),
                b"user:2".to_vec(),
                b"users".to_vec()
            ]
        );
        assert!(index.scan_prefix(b"nothing").is_empty());
        assert_eq!(index.scan_prefix(b"").len(), 7, "empty prefix = everything");
    }
}
