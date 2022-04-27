//! AWS S3 storage wrapper around `rusoto` library.
//!
//! Respects `prefix_in_bucket` property from [`S3Config`],
//! allowing multiple pageservers to independently work with the same S3 bucket, if
//! their bucket prefixes are both specified and different.

use std::path::{Path, PathBuf};

use anyhow::Context;
use rusoto_core::{
    credential::{InstanceMetadataProvider, StaticProvider},
    HttpClient, Region,
};
use rusoto_s3::{
    DeleteObjectRequest, GetObjectRequest, ListObjectsV2Request, PutObjectRequest, S3Client,
    StreamingBody, S3,
};
use tokio::{io, sync::Semaphore};
use tokio_util::io::ReaderStream;
use tracing::debug;

use crate::{
    config::S3Config,
    remote_storage::{strip_path_prefix, RemoteStorage},
};

use super::StorageMetadata;

const S3_FILE_SEPARATOR: char = '/';

#[derive(Debug, Eq, PartialEq)]
pub struct S3ObjectKey(String);

impl S3ObjectKey {
    fn key(&self) -> &str {
        &self.0
    }

    fn download_destination(
        &self,
        source_directory: &Path,
        prefix_to_strip: Option<&str>,
    ) -> PathBuf {
        let path_without_prefix = match prefix_to_strip {
            Some(prefix) => self.0.strip_prefix(prefix).unwrap_or_else(|| {
                panic!(
                    "Could not strip prefix '{}' from S3 object key '{}'",
                    prefix, self.0
                )
            }),
            None => &self.0,
        };

        source_directory.join(
            path_without_prefix
                .split(S3_FILE_SEPARATOR)
                .collect::<PathBuf>(),
        )
    }
}

/// AWS S3 storage.
pub struct S3Bucket {
    source_directory: &'static Path,
    client: S3Client,
    bucket_name: String,
    prefix_in_bucket: Option<String>,
    // Every request to S3 can be throttled or cancelled, if a certain number of requests per second is exceeded.
    // Same goes to IAM, which is queried before every S3 request, if enabled. IAM has even lower RPS threshold.
    // The helps to ensure we don't exceed the thresholds.
    concurrency_limiter: Semaphore,
}

impl S3Bucket {
    /// Creates the S3 storage, errors if incorrect AWS S3 configuration provided.
    pub fn new(source_directory: &'static Path, aws_config: &S3Config) -> anyhow::Result<Self> {
        debug!(
            "Creating s3 remote storage for S3 bucket {}",
            aws_config.bucket_name
        );
        let region = match aws_config.endpoint.clone() {
            Some(custom_endpoint) => Region::Custom {
                name: aws_config.bucket_region.clone(),
                endpoint: custom_endpoint,
            },
            None => aws_config
                .bucket_region
                .parse::<Region>()
                .context("Failed to parse the s3 region from config")?,
        };
        let request_dispatcher = HttpClient::new().context("Failed to create S3 http client")?;
        let client = if aws_config.access_key_id.is_none() && aws_config.secret_access_key.is_none()
        {
            debug!("Using IAM-based AWS access");
            S3Client::new_with(request_dispatcher, InstanceMetadataProvider::new(), region)
        } else {
            debug!("Using credentials-based AWS access");
            S3Client::new_with(
                request_dispatcher,
                StaticProvider::new_minimal(
                    aws_config.access_key_id.clone().unwrap_or_default(),
                    aws_config.secret_access_key.clone().unwrap_or_default(),
                ),
                region,
            )
        };

        let prefix_in_bucket = aws_config.prefix_in_bucket.as_deref().map(|prefix| {
            let mut prefix = prefix;
            while prefix.starts_with(S3_FILE_SEPARATOR) {
                prefix = &prefix[1..]
            }

            let mut prefix = prefix.to_string();
            while prefix.ends_with(S3_FILE_SEPARATOR) {
                prefix.pop();
            }
            prefix
        });

        Ok(Self {
            client,
            source_directory,
            bucket_name: aws_config.bucket_name.clone(),
            prefix_in_bucket,
            concurrency_limiter: Semaphore::new(aws_config.concurrency_limit.get()),
        })
    }
}

#[async_trait::async_trait]
impl RemoteStorage for S3Bucket {
    type StoragePath = S3ObjectKey;

    fn storage_path(&self, local_path: &Path) -> anyhow::Result<Self::StoragePath> {
        let relative_path = strip_path_prefix(self.source_directory, local_path)?;
        let mut key = self.prefix_in_bucket.clone().unwrap_or_default();
        for segment in relative_path {
            key.push(S3_FILE_SEPARATOR);
            key.push_str(&segment.to_string_lossy());
        }
        Ok(S3ObjectKey(key))
    }

    fn local_path(&self, storage_path: &Self::StoragePath) -> anyhow::Result<PathBuf> {
        Ok(storage_path
            .download_destination(self.source_directory, self.prefix_in_bucket.as_deref()))
    }

    async fn list(&self) -> anyhow::Result<Vec<Self::StoragePath>> {
        let mut document_keys = Vec::new();

        let mut continuation_token = None;
        loop {
            let _guard = self
                .concurrency_limiter
                .acquire()
                .await
                .context("Concurrency limiter semaphore got closed during S3 list")?;
            let fetch_response = self
                .client
                .list_objects_v2(ListObjectsV2Request {
                    bucket: self.bucket_name.clone(),
                    prefix: self.prefix_in_bucket.clone(),
                    continuation_token,
                    ..ListObjectsV2Request::default()
                })
                .await?;
            document_keys.extend(
                fetch_response
                    .contents
                    .unwrap_or_default()
                    .into_iter()
                    .filter_map(|o| Some(S3ObjectKey(o.key?))),
            );

            match fetch_response.continuation_token {
                Some(new_token) => continuation_token = Some(new_token),
                None => break,
            }
        }

        Ok(document_keys)
    }

    async fn upload(
        &self,
        from: impl io::AsyncRead + Unpin + Send + Sync + 'static,
        from_size_bytes: usize,
        to: &Self::StoragePath,
        metadata: Option<StorageMetadata>,
    ) -> anyhow::Result<()> {
        let _guard = self
            .concurrency_limiter
            .acquire()
            .await
            .context("Concurrency limiter semaphore got closed during S3 upload")?;
        self.client
            .put_object(PutObjectRequest {
                body: Some(StreamingBody::new_with_size(
                    ReaderStream::new(from),
                    from_size_bytes,
                )),
                bucket: self.bucket_name.clone(),
                key: to.key().to_owned(),
                metadata: metadata.map(|m| m.0),
                ..PutObjectRequest::default()
            })
            .await?;
        Ok(())
    }

    async fn download(
        &self,
        from: &Self::StoragePath,
        to: &mut (impl io::AsyncWrite + Unpin + Send + Sync),
    ) -> anyhow::Result<Option<StorageMetadata>> {
        let _guard = self
            .concurrency_limiter
            .acquire()
            .await
            .context("Concurrency limiter semaphore got closed during S3 download")?;
        let object_output = self
            .client
            .get_object(GetObjectRequest {
                bucket: self.bucket_name.clone(),
                key: from.key().to_owned(),
                ..GetObjectRequest::default()
            })
            .await?;

        if let Some(body) = object_output.body {
            let mut from = io::BufReader::new(body.into_async_read());
            io::copy(&mut from, to).await?;
        }

        Ok(object_output.metadata.map(StorageMetadata))
    }

    async fn download_range(
        &self,
        from: &Self::StoragePath,
        start_inclusive: u64,
        end_exclusive: Option<u64>,
        to: &mut (impl io::AsyncWrite + Unpin + Send + Sync),
    ) -> anyhow::Result<Option<StorageMetadata>> {
        // S3 accepts ranges as https://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.35
        // and needs both ends to be exclusive
        let end_inclusive = end_exclusive.map(|end| end.saturating_sub(1));
        let range = Some(match end_inclusive {
            Some(end_inclusive) => format!("bytes={}-{}", start_inclusive, end_inclusive),
            None => format!("bytes={}-", start_inclusive),
        });
        let _guard = self
            .concurrency_limiter
            .acquire()
            .await
            .context("Concurrency limiter semaphore got closed during S3 range download")?;
        let object_output = self
            .client
            .get_object(GetObjectRequest {
                bucket: self.bucket_name.clone(),
                key: from.key().to_owned(),
                range,
                ..GetObjectRequest::default()
            })
            .await?;

        if let Some(body) = object_output.body {
            let mut from = io::BufReader::new(body.into_async_read());
            io::copy(&mut from, to).await?;
        }

        Ok(object_output.metadata.map(StorageMetadata))
    }

    async fn delete(&self, path: &Self::StoragePath) -> anyhow::Result<()> {
        let _guard = self
            .concurrency_limiter
            .acquire()
            .await
            .context("Concurrency limiter semaphore got closed during S3 delete")?;
        self.client
            .delete_object(DeleteObjectRequest {
                bucket: self.bucket_name.clone(),
                key: path.key().to_owned(),
                ..DeleteObjectRequest::default()
            })
            .await?;
        Ok(())
    }
}