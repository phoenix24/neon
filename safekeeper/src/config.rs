// More copy pasta from pageserver/remote storage

use std::{num::{NonZeroUsize, NonZeroU32}, path::PathBuf};


/// AWS S3 bucket coordinates and access credentials to manage the bucket contents (read and write).
#[derive(Clone, PartialEq, Eq)]
pub struct S3Config {
    /// Name of the bucket to connect to.
    pub bucket_name: String,
    /// The region where the bucket is located at.
    pub bucket_region: String,
    /// A "subfolder" in the bucket, to use the same bucket separately by multiple pageservers at once.
    pub prefix_in_bucket: Option<String>,
    /// "Login" to use when connecting to bucket.
    /// Can be empty for cases like AWS k8s IAM
    /// where we can allow certain pods to connect
    /// to the bucket directly without any credentials.
    pub access_key_id: Option<String>,
    /// "Password" to use when connecting to bucket.
    pub secret_access_key: Option<String>,
    /// A base URL to send S3 requests to.
    /// By default, the endpoint is derived from a region name, assuming it's
    /// an AWS S3 region name, erroring on wrong region name.
    /// Endpoint provides a way to support other S3 flavors and their regions.
    ///
    /// Example: `http://127.0.0.1:5000`
    pub endpoint: Option<String>,
    /// AWS S3 has various limits on its API calls, we need not to exceed those.
    /// See [`defaults::DEFAULT_REMOTE_STORAGE_S3_CONCURRENCY_LIMIT`] for more details.
    pub concurrency_limit: NonZeroUsize,
}

impl std::fmt::Debug for S3Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3Config")
            .field("bucket_name", &self.bucket_name)
            .field("bucket_region", &self.bucket_region)
            .field("prefix_in_bucket", &self.prefix_in_bucket)
            .field("concurrency_limit", &self.concurrency_limit)
            .finish()
    }
}

/// External backup storage configuration, enough for creating a client for that storage.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteStorageConfig {
    /// Max allowed number of concurrent sync operations between pageserver and the remote storage.
    pub max_concurrent_timelines_sync: NonZeroUsize,
    /// Max allowed errors before the sync task is considered failed and evicted.
    pub max_sync_errors: NonZeroU32,
    /// The storage connection configuration.
    pub storage: RemoteStorageKind,
}

/// A kind of a remote storage to connect to, with its connection configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RemoteStorageKind {
    /// Storage based on local file system.
    /// Specify a root folder to place all stored files into.
    LocalFs(PathBuf),
    /// AWS S3 based storage, storing all files in the S3 bucket
    /// specified by the config
    AwsS3(S3Config),
    /// Noop storage
    NoopStorage(),
}