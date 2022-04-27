//! User credentials used in authentication.

use super::AuthError;
use crate::compute;
use crate::config::ProxyConfig;
use crate::error::UserFacingError;
use crate::stream::PqStream;
use std::collections::HashMap;
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncWrite};

#[derive(Debug, Error)]
pub enum ClientCredsParseError {
    #[error("Parameter `{0}` is missing in startup packet")]
    MissingKey(&'static str),
}

impl UserFacingError for ClientCredsParseError {}

/// Various client credentials which we use for authentication.
/// Note that we don't store any kind of client key or password here.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientCredentials {
    pub user: String,
    pub dbname: String,
}

impl TryFrom<HashMap<String, String>> for ClientCredentials {
    type Error = ClientCredsParseError;

    fn try_from(mut value: HashMap<String, String>) -> Result<Self, Self::Error> {
        let mut get_param = |key| {
            value
                .remove(key)
                .ok_or(ClientCredsParseError::MissingKey(key))
        };

        let user = get_param("user")?;
        let db = get_param("database")?;

        Ok(Self { user, dbname: db })
    }
}

impl ClientCredentials {
    /// Use credentials to authenticate the user.
    pub async fn authenticate(
        self,
        config: &ProxyConfig,
        client: &mut PqStream<impl AsyncRead + AsyncWrite + Unpin>,
    ) -> Result<compute::NodeInfo, AuthError> {
        if self.user.ends_with("@zenith") {
            let cloud = config.cloud_endpoint.as_ref();
            super::handle_existing_user(cloud, client, self).await
        } else {
            let redirect_uri = config.redirect_uri.as_str();
            super::handle_new_user(redirect_uri, client).await
        }
    }
}
