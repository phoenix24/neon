//! Timeline synchronization logic to compress and upload to the remote storage all new timeline files from the checkpoints.

use std::{fmt::Debug, path::PathBuf};

use anyhow::Context;
use futures::stream::{FuturesUnordered, StreamExt};
use tokio::fs;
use tracing::{debug, error, info, warn};

use crate::{
    config::PageServerConf,
    layered_repository::metadata::metadata_path,
    remote_storage::{
        storage_sync::{index::RemoteTimeline, sync_queue, SyncTask},
        RemoteStorage,
    },
};
use utils::zid::ZTenantTimelineId;

use super::{index::IndexPart, SyncData, TimelineUpload};

/// Serializes and uploads the given index part data to the remote storage.
pub(super) async fn upload_index_part<P, S>(
    conf: &'static PageServerConf,
    storage: &S,
    sync_id: ZTenantTimelineId,
    index_part: IndexPart,
) -> anyhow::Result<()>
where
    P: Debug + Send + Sync + 'static,
    S: RemoteStorage<StoragePath = P> + Send + Sync + 'static,
{
    let index_part_bytes = serde_json::to_vec(&index_part)
        .context("Failed to serialize index part file into bytes")?;
    let index_part_size = index_part_bytes.len();
    let index_part_bytes = tokio::io::BufReader::new(std::io::Cursor::new(index_part_bytes));

    let index_part_path = metadata_path(conf, sync_id.timeline_id, sync_id.tenant_id)
        .with_file_name(IndexPart::FILE_NAME)
        .with_extension(IndexPart::FILE_EXTENSION);
    let index_part_storage_path = storage.storage_path(&index_part_path).with_context(|| {
        format!(
            "Failed to get the index part storage path for local path '{}'",
            index_part_path.display()
        )
    })?;

    storage
        .upload(
            index_part_bytes,
            index_part_size,
            &index_part_storage_path,
            None,
        )
        .await
        .with_context(|| {
            format!("Failed to upload index part to the storage path '{index_part_storage_path:?}'")
        })
}

/// Timeline upload result, with extra data, needed for uploading.
#[derive(Debug)]
pub(super) enum UploadedTimeline {
    /// Upload failed due to some error, the upload task is rescheduled for another retry.
    FailedAndRescheduled,
    /// No issues happened during the upload, all task files were put into the remote storage.
    Successful(SyncData<TimelineUpload>),
    /// No failures happened during the upload, but some files were removed locally before the upload task completed
    /// (could happen due to retries, for instance, if GC happens in the interim).
    /// Such files are considered "not needed" and ignored, but the task's metadata should be discarded and the new one loaded from the local file.
    SuccessfulAfterLocalFsUpdate(SyncData<TimelineUpload>),
}

/// Attempts to upload given layer files.
/// No extra checks for overlapping files is made and any files that are already present remotely will be overwritten, if submitted during the upload.
///
/// On an error, bumps the retries count and reschedules the entire task.
pub(super) async fn upload_timeline_layers<'a, P, S>(
    storage: &'a S,
    remote_timeline: Option<&'a RemoteTimeline>,
    sync_id: ZTenantTimelineId,
    mut upload_data: SyncData<TimelineUpload>,
) -> UploadedTimeline
where
    P: Debug + Send + Sync + 'static,
    S: RemoteStorage<StoragePath = P> + Send + Sync + 'static,
{
    let upload = &mut upload_data.data;
    let new_upload_lsn = upload.metadata.disk_consistent_lsn();

    let already_uploaded_layers = remote_timeline
        .map(|timeline| timeline.stored_files())
        .cloned()
        .unwrap_or_default();

    let layers_to_upload = upload
        .layers_to_upload
        .difference(&already_uploaded_layers)
        .cloned()
        .collect::<Vec<_>>();

    debug!("Layers to upload: {layers_to_upload:?}");
    info!(
        "Uploading {} timeline layers, new lsn: {new_upload_lsn}",
        layers_to_upload.len(),
    );

    let mut upload_tasks = layers_to_upload
        .into_iter()
        .map(|source_path| async move {
            let storage_path = storage
                .storage_path(&source_path)
                .with_context(|| {
                    format!(
                        "Failed to get the layer storage path for local path '{}'",
                        source_path.display()
                    )
                })
                .map_err(UploadError::Other)?;

            let source_file = match fs::File::open(&source_path).await.with_context(|| {
                format!(
                    "Failed to upen a source file for layer '{}'",
                    source_path.display()
                )
            }) {
                Ok(file) => file,
                Err(e) => return Err(UploadError::MissingLocalFile(source_path, e)),
            };

            let source_size = source_file
                .metadata()
                .await
                .with_context(|| {
                    format!(
                        "Failed to get the source file metadata for layer '{}'",
                        source_path.display()
                    )
                })
                .map_err(UploadError::Other)?
                .len() as usize;

            match storage
                .upload(source_file, source_size, &storage_path, None)
                .await
                .with_context(|| {
                    format!(
                        "Failed to upload a layer from local path '{}'",
                        source_path.display()
                    )
                }) {
                Ok(()) => Ok(source_path),
                Err(e) => Err(UploadError::MissingLocalFile(source_path, e)),
            }
        })
        .collect::<FuturesUnordered<_>>();

    let mut errors_happened = false;
    let mut local_fs_updated = false;
    while let Some(upload_result) = upload_tasks.next().await {
        match upload_result {
            Ok(uploaded_path) => {
                upload.layers_to_upload.remove(&uploaded_path);
                upload.uploaded_layers.insert(uploaded_path);
            }
            Err(e) => match e {
                UploadError::Other(e) => {
                    errors_happened = true;
                    error!("Failed to upload a layer for timeline {sync_id}: {e:?}");
                }
                UploadError::MissingLocalFile(source_path, e) => {
                    if source_path.exists() {
                        errors_happened = true;
                        error!("Failed to upload a layer for timeline {sync_id}: {e:?}");
                    } else {
                        local_fs_updated = true;
                        upload.layers_to_upload.remove(&source_path);
                        warn!(
                            "Missing locally a layer file {} scheduled for upload, skipping",
                            source_path.display()
                        );
                    }
                }
            },
        }
    }

    if errors_happened {
        debug!("Reenqueuing failed upload task for timeline {sync_id}");
        upload_data.retries += 1;
        sync_queue::push(sync_id, SyncTask::Upload(upload_data));
        UploadedTimeline::FailedAndRescheduled
    } else if local_fs_updated {
        info!("Successfully uploaded all layers, some local layers were removed during the upload");
        UploadedTimeline::SuccessfulAfterLocalFsUpdate(upload_data)
    } else {
        info!("Successfully uploaded all layers");
        UploadedTimeline::Successful(upload_data)
    }
}

enum UploadError {
    MissingLocalFile(PathBuf, anyhow::Error),
    Other(anyhow::Error),
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeSet, HashSet};

    use tempfile::tempdir;
    use utils::lsn::Lsn;

    use crate::{
        remote_storage::{
            storage_sync::{
                index::RelativePath,
                test_utils::{create_local_timeline, dummy_metadata},
            },
            LocalFs,
        },
        repository::repo_harness::{RepoHarness, TIMELINE_ID},
    };

    use super::{upload_index_part, *};

    #[tokio::test]
    async fn regular_layer_upload() -> anyhow::Result<()> {
        let harness = RepoHarness::create("regular_layer_upload")?;
        let sync_id = ZTenantTimelineId::new(harness.tenant_id, TIMELINE_ID);

        let layer_files = ["a", "b"];
        let storage = LocalFs::new(tempdir()?.path().to_path_buf(), &harness.conf.workdir)?;
        let current_retries = 3;
        let metadata = dummy_metadata(Lsn(0x30));
        let local_timeline_path = harness.timeline_path(&TIMELINE_ID);
        let timeline_upload =
            create_local_timeline(&harness, TIMELINE_ID, &layer_files, metadata.clone()).await?;
        assert!(
            storage.list().await?.is_empty(),
            "Storage should be empty before any uploads are made"
        );

        let upload_result = upload_timeline_layers(
            &storage,
            None,
            sync_id,
            SyncData::new(current_retries, timeline_upload.clone()),
        )
        .await;

        let upload_data = match upload_result {
            UploadedTimeline::Successful(upload_data) => upload_data,
            wrong_result => {
                panic!("Expected a successful upload for timeline, but got: {wrong_result:?}")
            }
        };

        assert_eq!(
            current_retries, upload_data.retries,
            "On successful upload, retries are not expected to change"
        );
        let upload = &upload_data.data;
        assert!(
            upload.layers_to_upload.is_empty(),
            "Successful upload should have no layers left to upload"
        );
        assert_eq!(
            upload
                .uploaded_layers
                .iter()
                .cloned()
                .collect::<BTreeSet<_>>(),
            layer_files
                .iter()
                .map(|layer_file| local_timeline_path.join(layer_file))
                .collect(),
            "Successful upload should have all layers uploaded"
        );
        assert_eq!(
            upload.metadata, metadata,
            "Successful upload should not chage its metadata"
        );

        let storage_files = storage.list().await?;
        assert_eq!(
            storage_files.len(),
            layer_files.len(),
            "All layers should be uploaded"
        );
        assert_eq!(
            storage_files
                .into_iter()
                .map(|storage_path| storage.local_path(&storage_path))
                .collect::<anyhow::Result<BTreeSet<_>>>()?,
            layer_files
                .into_iter()
                .map(|file| local_timeline_path.join(file))
                .collect(),
            "Uploaded files should match with the local ones"
        );

        Ok(())
    }

    // Currently, GC can run between upload retries, removing local layers scheduled for upload. Test this scenario.
    #[tokio::test]
    async fn layer_upload_after_local_fs_update() -> anyhow::Result<()> {
        let harness = RepoHarness::create("layer_upload_after_local_fs_update")?;
        let sync_id = ZTenantTimelineId::new(harness.tenant_id, TIMELINE_ID);

        let layer_files = ["a1", "b1"];
        let storage = LocalFs::new(tempdir()?.path().to_owned(), &harness.conf.workdir)?;
        let current_retries = 5;
        let metadata = dummy_metadata(Lsn(0x40));

        let local_timeline_path = harness.timeline_path(&TIMELINE_ID);
        let layers_to_upload = {
            let mut layers = layer_files.to_vec();
            layers.push("layer_to_remove");
            layers
        };
        let timeline_upload =
            create_local_timeline(&harness, TIMELINE_ID, &layers_to_upload, metadata.clone())
                .await?;
        assert!(
            storage.list().await?.is_empty(),
            "Storage should be empty before any uploads are made"
        );

        fs::remove_file(local_timeline_path.join("layer_to_remove")).await?;

        let upload_result = upload_timeline_layers(
            &storage,
            None,
            sync_id,
            SyncData::new(current_retries, timeline_upload.clone()),
        )
        .await;

        let upload_data = match upload_result {
            UploadedTimeline::SuccessfulAfterLocalFsUpdate(upload_data) => upload_data,
            wrong_result => panic!(
                "Expected a successful after local fs upload for timeline, but got: {wrong_result:?}"
            ),
        };

        assert_eq!(
            current_retries, upload_data.retries,
            "On successful upload, retries are not expected to change"
        );
        let upload = &upload_data.data;
        assert!(
                upload.layers_to_upload.is_empty(),
                "Successful upload should have no layers left to upload, even those that were removed from the local fs"
            );
        assert_eq!(
            upload
                .uploaded_layers
                .iter()
                .cloned()
                .collect::<BTreeSet<_>>(),
            layer_files
                .iter()
                .map(|layer_file| local_timeline_path.join(layer_file))
                .collect(),
            "Successful upload should have all layers uploaded"
        );
        assert_eq!(
            upload.metadata, metadata,
            "Successful upload should not chage its metadata"
        );

        let storage_files = storage.list().await?;
        assert_eq!(
            storage_files.len(),
            layer_files.len(),
            "All layers should be uploaded"
        );
        assert_eq!(
            storage_files
                .into_iter()
                .map(|storage_path| storage.local_path(&storage_path))
                .collect::<anyhow::Result<BTreeSet<_>>>()?,
            layer_files
                .into_iter()
                .map(|file| local_timeline_path.join(file))
                .collect(),
            "Uploaded files should match with the local ones"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_upload_index_part() -> anyhow::Result<()> {
        let harness = RepoHarness::create("test_upload_index_part")?;
        let sync_id = ZTenantTimelineId::new(harness.tenant_id, TIMELINE_ID);

        let storage = LocalFs::new(tempdir()?.path().to_owned(), &harness.conf.workdir)?;
        let metadata = dummy_metadata(Lsn(0x40));
        let local_timeline_path = harness.timeline_path(&TIMELINE_ID);

        let index_part = IndexPart::new(
            HashSet::from([
                RelativePath::new(&local_timeline_path, local_timeline_path.join("one"))?,
                RelativePath::new(&local_timeline_path, local_timeline_path.join("two"))?,
            ]),
            HashSet::from([RelativePath::new(
                &local_timeline_path,
                local_timeline_path.join("three"),
            )?]),
            metadata.disk_consistent_lsn(),
            metadata.to_bytes()?,
        );

        assert!(
            storage.list().await?.is_empty(),
            "Storage should be empty before any uploads are made"
        );
        upload_index_part(harness.conf, &storage, sync_id, index_part.clone()).await?;

        let storage_files = storage.list().await?;
        assert_eq!(
            storage_files.len(),
            1,
            "Should have only the index part file uploaded"
        );

        let index_part_path = storage_files.first().unwrap();
        assert_eq!(
            index_part_path.file_stem().and_then(|name| name.to_str()),
            Some(IndexPart::FILE_NAME),
            "Remote index part should have the correct name"
        );
        assert_eq!(
            index_part_path
                .extension()
                .and_then(|extension| extension.to_str()),
            Some(IndexPart::FILE_EXTENSION),
            "Remote index part should have the correct extension"
        );

        let remote_index_part: IndexPart =
            serde_json::from_slice(&fs::read(&index_part_path).await?)?;
        assert_eq!(
            index_part, remote_index_part,
            "Remote index part should match the local one"
        );

        Ok(())
    }
}
