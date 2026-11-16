#![allow(dead_code)]

use std::path::{Path, PathBuf};

pub struct ScratchDir {
    path: PathBuf,
}

impl ScratchDir {
    pub fn new(name: &str) -> Self {
        let path =
            std::env::temp_dir().join(format!("rust_database_{name}_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&path);
        Self { path }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for ScratchDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

pub fn text(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}
