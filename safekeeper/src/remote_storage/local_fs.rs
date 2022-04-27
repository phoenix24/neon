//! Local filesystem acting as a remote storage.
//! Multiple pageservers can use the same "storage" of this kind by using different storage roots.
//!
//! This storage used in pageserver tests, but can also be used in cases when a certain persistent
//! volume is mounted to the local FS.

use std::{
    future::Future,
    path::{Path, PathBuf},
    pin::Pin,
};

use anyhow::{bail, ensure, Context};
use tokio::{
    fs,
    io::{self, AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
};
use tracing::*;

use super::{strip_path_prefix, RemoteStorage, StorageMetadata};

pub struct LocalFs {
    source_directory: &'static Path,
    backup_destination: PathBuf,
}

impl LocalFs {
    /// Attempts to create local FS storage, along with its root directory.
    pub fn new(source_directory: &'static Path, backup_destination: PathBuf) -> anyhow::Result<Self> {
        if !backup_destination.exists() {
            std::fs::create_dir_all(&backup_destination).with_context(|| {
                format!(
                    "Failed to create all directories in the given root path '{}'",
                    backup_destination.display(),
                )
            })?;
        }
        Ok(Self {
            source_directory,
            backup_destination,
        })
    }

    fn resolve_in_storage(&self, path: &Path) -> anyhow::Result<PathBuf> {
        if path.is_relative() {
            Ok(self.backup_destination.join(path))
        } else if path.starts_with(&self.backup_destination) {
            Ok(path.to_path_buf())
        } else {
            bail!(
                "Path '{}' does not belong to the current storage",
                path.display()
            )
        }
    }

    async fn read_storage_metadata(
        &self,
        file_path: &Path,
    ) -> anyhow::Result<Option<StorageMetadata>> {
        let metadata_path = storage_metadata_path(file_path);
        if metadata_path.exists() && metadata_path.is_file() {
            let metadata_string = fs::read_to_string(&metadata_path).await.with_context(|| {
                format!(
                    "Failed to read metadata from the local storage at '{}'",
                    metadata_path.display()
                )
            })?;

            serde_json::from_str(&metadata_string)
                .with_context(|| {
                    format!(
                        "Failed to deserialize metadata from the local storage at '{}'",
                        metadata_path.display()
                    )
                })
                .map(|metadata| Some(StorageMetadata(metadata)))
        } else {
            Ok(None)
        }
    }
}

#[async_trait::async_trait]
impl RemoteStorage for LocalFs {
    type StoragePath = PathBuf;

    fn storage_path(&self, local_path: &Path) -> anyhow::Result<Self::StoragePath> {
        Ok(self.backup_destination.join(
            strip_path_prefix(self.source_directory, local_path)
                .context("local path does not belong to this storage")?,
        ))
    }

    fn local_path(&self, storage_path: &Self::StoragePath) -> anyhow::Result<PathBuf> {
        let relative_path = strip_path_prefix(&self.backup_destination, storage_path)
            .context("local path does not belong to this storage")?;
        Ok(self.source_directory.join(relative_path))
    }

    async fn list(&self) -> anyhow::Result<Vec<Self::StoragePath>> {
        get_all_files(&self.backup_destination).await
    }

    async fn upload(
        &self,
        from: impl io::AsyncRead + Unpin + Send + Sync + 'static,
        from_size_bytes: usize,
        to: &Self::StoragePath,
        metadata: Option<StorageMetadata>,
    ) -> anyhow::Result<()> {
        let target_file_path = self.resolve_in_storage(to)?;
        create_target_directory(&target_file_path).await?;
        // We need this dance with sort of durable rename (without fsyncs)
        // to prevent partial uploads. This was really hit when pageserver shutdown
        // cancelled the upload and partial file was left on the fs
        let temp_file_path = path_with_suffix_extension(&target_file_path, ".temp");
        let mut destination = io::BufWriter::new(
            fs::OpenOptions::new()
                .write(true)
                .create(true)
                .open(&temp_file_path)
                .await
                .with_context(|| {
                    format!(
                        "Failed to open target fs destination at '{}'",
                        target_file_path.display()
                    )
                })?,
        );

        let from_size_bytes = from_size_bytes as u64;
        // Require to read 1 byte more than the expected to check later, that the stream and its size match.
        let mut buffer_to_read = from.take(from_size_bytes + 1);

        let bytes_read = io::copy(&mut buffer_to_read, &mut destination)
            .await
            .with_context(|| {
                format!(
                    "Failed to upload file (write temp) to the local storage at '{}'",
                    temp_file_path.display()
                )
            })?;

        ensure!(
            bytes_read == from_size_bytes,
            "Provided stream has actual size {} fthat is smaller than the given stream size {}",
            bytes_read,
            from_size_bytes
        );

        ensure!(
            buffer_to_read.read(&mut [0]).await? == 0,
            "Provided stream has bigger size than the given stream size {}",
            from_size_bytes
        );

        destination.flush().await.with_context(|| {
            format!(
                "Failed to upload (flush temp) file to the local storage at '{}'",
                temp_file_path.display()
            )
        })?;

        fs::rename(temp_file_path, &target_file_path)
            .await
            .with_context(|| {
                format!(
                    "Failed to upload (rename) file to the local storage at '{}'",
                    target_file_path.display()
                )
            })?;

        if let Some(storage_metadata) = metadata {
            let storage_metadata_path = storage_metadata_path(&target_file_path);
            fs::write(
                &storage_metadata_path,
                serde_json::to_string(&storage_metadata.0)
                    .context("Failed to serialize storage metadata as json")?,
            )
            .await
            .with_context(|| {
                format!(
                    "Failed to write metadata to the local storage at '{}'",
                    storage_metadata_path.display()
                )
            })?;
        }

        Ok(())
    }

    async fn download(
        &self,
        from: &Self::StoragePath,
        to: &mut (impl io::AsyncWrite + Unpin + Send + Sync),
    ) -> anyhow::Result<Option<StorageMetadata>> {
        let file_path = self.resolve_in_storage(from)?;

        if file_path.exists() && file_path.is_file() {
            let mut source = io::BufReader::new(
                fs::OpenOptions::new()
                    .read(true)
                    .open(&file_path)
                    .await
                    .with_context(|| {
                        format!(
                            "Failed to open source file '{}' to use in the download",
                            file_path.display()
                        )
                    })?,
            );
            io::copy(&mut source, to).await.with_context(|| {
                format!(
                    "Failed to download file '{}' from the local storage",
                    file_path.display()
                )
            })?;
            source.flush().await?;

            self.read_storage_metadata(&file_path).await
        } else {
            bail!(
                "File '{}' either does not exist or is not a file",
                file_path.display()
            )
        }
    }

    async fn download_range(
        &self,
        from: &Self::StoragePath,
        start_inclusive: u64,
        end_exclusive: Option<u64>,
        to: &mut (impl io::AsyncWrite + Unpin + Send + Sync),
    ) -> anyhow::Result<Option<StorageMetadata>> {
        if let Some(end_exclusive) = end_exclusive {
            ensure!(
                end_exclusive > start_inclusive,
                "Invalid range, start ({}) is bigger then end ({:?})",
                start_inclusive,
                end_exclusive
            );
            if start_inclusive == end_exclusive.saturating_sub(1) {
                return Ok(None);
            }
        }
        let file_path = self.resolve_in_storage(from)?;

        if file_path.exists() && file_path.is_file() {
            let mut source = io::BufReader::new(
                fs::OpenOptions::new()
                    .read(true)
                    .open(&file_path)
                    .await
                    .with_context(|| {
                        format!(
                            "Failed to open source file '{}' to use in the download",
                            file_path.display()
                        )
                    })?,
            );
            source
                .seek(io::SeekFrom::Start(start_inclusive))
                .await
                .context("Failed to seek to the range start in a local storage file")?;
            match end_exclusive {
                Some(end_exclusive) => {
                    io::copy(&mut source.take(end_exclusive - start_inclusive), to).await
                }
                None => io::copy(&mut source, to).await,
            }
            .with_context(|| {
                format!(
                    "Failed to download file '{}' range from the local storage",
                    file_path.display()
                )
            })?;

            self.read_storage_metadata(&file_path).await
        } else {
            bail!(
                "File '{}' either does not exist or is not a file",
                file_path.display()
            )
        }
    }

    async fn delete(&self, path: &Self::StoragePath) -> anyhow::Result<()> {
        let file_path = self.resolve_in_storage(path)?;
        if file_path.exists() && file_path.is_file() {
            Ok(fs::remove_file(file_path).await?)
        } else {
            bail!(
                "File '{}' either does not exist or is not a file",
                file_path.display()
            )
        }
    }
}

fn path_with_suffix_extension(original_path: &Path, suffix: &str) -> PathBuf {
    let mut extension_with_suffix = original_path.extension().unwrap_or_default().to_os_string();
    extension_with_suffix.push(suffix);

    original_path.with_extension(extension_with_suffix)
}

fn storage_metadata_path(original_path: &Path) -> PathBuf {
    path_with_suffix_extension(original_path, ".metadata")
}

fn get_all_files<'a, P>(
    directory_path: P,
) -> Pin<Box<dyn Future<Output = anyhow::Result<Vec<PathBuf>>> + Send + Sync + 'a>>
where
    P: AsRef<Path> + Send + Sync + 'a,
{
    Box::pin(async move {
        let directory_path = directory_path.as_ref();
        if directory_path.exists() {
            if directory_path.is_dir() {
                let mut paths = Vec::new();
                let mut dir_contents = fs::read_dir(directory_path).await?;
                while let Some(dir_entry) = dir_contents.next_entry().await? {
                    let file_type = dir_entry.file_type().await?;
                    let entry_path = dir_entry.path();
                    if file_type.is_symlink() {
                        debug!("{:?} us a symlink, skipping", entry_path)
                    } else if file_type.is_dir() {
                        paths.extend(get_all_files(entry_path).await?.into_iter())
                    } else {
                        paths.push(dir_entry.path());
                    }
                }
                Ok(paths)
            } else {
                bail!("Path '{}' is not a directory", directory_path.display())
            }
        } else {
            Ok(Vec::new())
        }
    })
}

async fn create_target_directory(target_file_path: &Path) -> anyhow::Result<()> {
    let target_dir = match target_file_path.parent() {
        Some(parent_dir) => parent_dir,
        None => bail!(
            "File path '{}' has no parent directory",
            target_file_path.display()
        ),
    };
    if !target_dir.exists() {
        fs::create_dir_all(target_dir).await?;
    }
    Ok(())
}