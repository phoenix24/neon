//! Noop implementation for backup

use std::{
    path::{Path, PathBuf},
};

use anyhow::{bail};
use tokio::{
    io::{self},
};
// use tracing::*;

use super::{RemoteStorage, StorageMetadata};

pub struct NoopStorage { }

impl NoopStorage {
    /// Attempts to create local FS storage, along with its root directory.
    pub fn new() -> anyhow::Result<Self> {
        Ok(Self {})
    }
}

#[async_trait::async_trait]
impl RemoteStorage for NoopStorage {
    type StoragePath = PathBuf;

    fn storage_path(&self, local_path: &Path) -> anyhow::Result<Self::StoragePath> {
        Ok(local_path.to_path_buf())
    }

    fn local_path(&self, storage_path: &Self::StoragePath) -> anyhow::Result<PathBuf> {
        Ok(storage_path.to_path_buf())
    }

    async fn list(&self) -> anyhow::Result<Vec<Self::StoragePath>> {
        Ok(Vec::new())
    }

    async fn upload(
        &self,
        _from: impl io::AsyncRead + Unpin + Send + Sync + 'static,
        _from_size_bytes: usize,
        _to: &Self::StoragePath,
        _metadata: Option<StorageMetadata>,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    async fn download(
        &self,
        _from: &Self::StoragePath,
        _to: &mut (impl io::AsyncWrite + Unpin + Send + Sync),
    ) -> anyhow::Result<Option<StorageMetadata>> {
        bail!("File either does not exist or is not a file")
    }

    async fn download_range(
        &self,
        _from: &Self::StoragePath,
        _start_inclusive: u64,
        _end_exclusive: Option<u64>,
        _to: &mut (impl io::AsyncWrite + Unpin + Send + Sync),
    ) -> anyhow::Result<Option<StorageMetadata>> {
        bail!("File either does not exist or is not a file")
    }

    async fn delete(&self, _path: &Self::StoragePath) -> anyhow::Result<()> {
        bail!("File either does not exist or is not a file")
    }
}

