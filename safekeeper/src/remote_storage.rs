/// This is a copy pasta from Page Server

mod local_fs;
mod s3_bucket;
mod noop_storage;

// use std::{
//     collections::{HashMap, HashSet},
//     ffi, fs,
//     path::{Path, PathBuf},
// };

// use anyhow::{bail, Context};
// use tokio::io;
// use tracing::{debug, error, info};

// pub use self::{
//     local_fs::LocalFs,
//     s3_bucket::S3Bucket,
// };

// use crate::{
//     config::RemoteStorageKind,
// };
// use utils::zid::{ZTenantId, ZTenantTimelineId, ZTimelineId};


use std::{
    collections::{HashMap},
    path::{Path, PathBuf},
};

use anyhow::{Context, bail, Result};
use tokio::io;
use tracing::{info};

use crate::config::{RemoteStorageConfig, RemoteStorageKind};

pub use self::{
    local_fs::LocalFs,
    s3_bucket::S3Bucket,
    noop_storage::NoopStorage,
};


/// Storage (potentially remote) API to manage its state.
/// This storage tries to be unaware of any layered repository context,
/// providing basic CRUD operations for storage files.
#[async_trait::async_trait]
pub trait RemoteStorage: Send + Sync {
    /// A way to uniquely reference a file in the remote storage.
    type StoragePath;

    /// Attempts to derive the storage path out of the local path, if the latter is correct.
    fn storage_path(&self, local_path: &Path) -> anyhow::Result<Self::StoragePath>;

    /// Gets the download path of the given storage file.
    fn local_path(&self, storage_path: &Self::StoragePath) -> anyhow::Result<PathBuf>;

    /// Lists all items the storage has right now.
    async fn list(&self) -> anyhow::Result<Vec<Self::StoragePath>>;

    /// Streams the local file contents into remote into the remote storage entry.
    async fn upload(
        &self,
        from: impl io::AsyncRead + Unpin + Send + Sync + 'static,
        // S3 PUT request requires the content length to be specified,
        // otherwise it starts to fail with the concurrent connection count increasing.
        from_size_bytes: usize,
        to: &Self::StoragePath,
        metadata: Option<StorageMetadata>,
    ) -> anyhow::Result<()>;

    /// Streams the remote storage entry contents into the buffered writer given, returns the filled writer.
    /// Returns the metadata, if any was stored with the file previously.
    async fn download(
        &self,
        from: &Self::StoragePath,
        to: &mut (impl io::AsyncWrite + Unpin + Send + Sync),
    ) -> anyhow::Result<Option<StorageMetadata>>;

    /// Streams a given byte range of the remote storage entry contents into the buffered writer given, returns the filled writer.
    /// Returns the metadata, if any was stored with the file previously.
    async fn download_range(
        &self,
        from: &Self::StoragePath,
        start_inclusive: u64,
        end_exclusive: Option<u64>,
        to: &mut (impl io::AsyncWrite + Unpin + Send + Sync),
    ) -> anyhow::Result<Option<StorageMetadata>>;

    async fn delete(&self, path: &Self::StoragePath) -> anyhow::Result<()>;
}

/// Extra set of key-value pairs that contain arbitrary metadata about the storage entry.
/// Immutable, cannot be changed once the file is created.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageMetadata(HashMap<String, String>);

fn strip_path_prefix<'a>(prefix: &'a Path, path: &'a Path) -> anyhow::Result<&'a Path> {
    if prefix == path {
        anyhow::bail!(
            "Prefix and the path are equal, cannot strip: '{}'",
            prefix.display()
        )
    } else {
        path.strip_prefix(prefix).with_context(|| {
            format!(
                "Path '{}' is not prefixed with '{}'",
                path.display(),
                prefix.display(),
            )
        })
    }
}


pub fn create(remote_storage_config: Option<&RemoteStorageConfig>, source_directory: PathBuf) -> Result<LocalFs> {
    if let Some(config) = remote_storage_config {
        return match &config.storage {
            RemoteStorageKind::LocalFs(destination) => {
                info!("Using fs root '{:?}' as a remote storage", destination);
                LocalFs::new(&source_directory, destination.to_path_buf())
            },
            _=> 
            bail!("can't create anything different")
        };
    }

    bail!("no remote storage config supplied")
}


// TODO: antons: can't figure out how to create this fucking storage
// pub fn create<S, P>(remote_storage_config: Option<&RemoteStorageConfig>, source_directory: PathBuf)
//  -> anyhow::Result<S> 
//  where 
//  P: std::fmt::Debug + Send + Sync + 'static,
//  S: RemoteStorage<StoragePath = P> + Send + Sync + 'static,
//  {

//     if let Some(config) = remote_storage_config {
//         let s:S = match &config.storage {
//             RemoteStorageKind::LocalFs(destination) => {
//                 info!("Using fs root '{:?}' as a remote storage", destination);
//                 LocalFs::new(&source_directory, destination.to_path_buf())
//             },
//             RemoteStorageKind::AwsS3(s3_config) => {
//                 info!("Using s3 bucket '{}' in region '{}' as a remote storage, prefix in bucket: '{:?}', bucket endpoint: '{:?}'",
//                     s3_config.bucket_name, s3_config.bucket_region, s3_config.prefix_in_bucket, s3_config.endpoint);
//                 S3Bucket::new(&source_directory, s3_config)?
//             },
//             RemoteStorageKind::NoopStorage() => {
//                 info!("Using noop as a remote storage");
//                 NoopStorage::new()
//             }
//         };

//         let _y = s;

//     } else {
//     }


//     Ok(())
// }