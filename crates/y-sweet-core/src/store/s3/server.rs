#![cfg(not(target_arch = "wasm32"))]

use crate::store::{Result, Store, StoreError};
use async_trait::async_trait;
use aws_config::retry::RetryConfig;
use aws_config::BehaviorVersion;
use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::Client;
use serde::{Deserialize, Serialize};
use tokio::sync::OnceCell;

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct ServerS3Config {
    pub bucket: String,
    pub bucket_prefix: Option<String>,
}

pub struct ServerS3Store {
    config: ServerS3Config,
    client: OnceCell<Client>,
    bucket_checked: OnceCell<()>,
}

impl ServerS3Store {
    pub fn new(config: ServerS3Config) -> Self {
        Self {
            config,
            client: OnceCell::new(),
            bucket_checked: OnceCell::new(),
        }
    }

    /// Lazily build the SDK client on first use. Subsequent calls return the
    /// cached client. The SDK reads `AWS_REGION`, `AWS_ENDPOINT_URL_S3`,
    /// `AWS_S3_USE_PATH_STYLE`, and the credential provider chain (env →
    /// profile → IRSA → ECS container → IMDS) itself; this code does not
    /// touch any AWS env vars.
    async fn client(&self) -> &Client {
        self.client
            .get_or_init(|| async {
                let sdk_config = aws_config::defaults(BehaviorVersion::latest())
                    .load()
                    .await;
                let s3_config = aws_sdk_s3::config::Builder::from(&sdk_config)
                    .retry_config(RetryConfig::adaptive().with_max_attempts(3))
                    .build();
                Client::from_conf(s3_config)
            })
            .await
    }

    fn prefixed_key(&self, key: &str) -> String {
        if let Some(p) = &self.config.bucket_prefix {
            format!("{}/{}", p, key)
        } else {
            key.to_string()
        }
    }
}

/// Map a generic `SdkError` to a `StoreError`, *excluding* per-operation
/// "not found" cases — callers should match those before calling this.
fn map_sdk_err<E>(err: SdkError<E>, op: &str) -> StoreError
where
    E: std::fmt::Debug,
{
    match err {
        SdkError::ServiceError(svc) => {
            let status = svc.raw().status().as_u16();
            match status {
                401 | 403 => StoreError::NotAuthorized(format!(
                    "{op}: HTTP {status} from S3 (access denied)"
                )),
                _ => StoreError::ConnectionError(format!(
                    "{op}: HTTP {status} service error: {:?}",
                    svc.err()
                )),
            }
        }
        SdkError::TimeoutError(_) => {
            StoreError::ConnectionError(format!("{op}: timeout"))
        }
        SdkError::DispatchFailure(e) => {
            StoreError::ConnectionError(format!("{op}: dispatch failure: {e:?}"))
        }
        SdkError::ResponseError(e) => {
            StoreError::ConnectionError(format!("{op}: response parse error: {e:?}"))
        }
        SdkError::ConstructionFailure(e) => {
            StoreError::ConnectionError(format!("{op}: request construction failure: {e:?}"))
        }
        other => StoreError::ConnectionError(format!("{op}: {other:?}")),
    }
}

#[async_trait]
impl Store for ServerS3Store {
    async fn init(&self) -> Result<()> {
        self.bucket_checked
            .get_or_try_init(|| async {
                match self
                    .client()
                    .await
                    .head_bucket()
                    .bucket(&self.config.bucket)
                    .send()
                    .await
                {
                    Ok(_) => Ok(()),
                    Err(SdkError::ServiceError(svc)) => {
                        let status = svc.raw().status().as_u16();
                        match status {
                            404 => Err(StoreError::BucketDoesNotExist(format!(
                                "Bucket {} does not exist",
                                self.config.bucket
                            ))),
                            401 | 403 => Err(StoreError::NotAuthorized(format!(
                                "head_bucket: HTTP {status} from S3"
                            ))),
                            _ => Err(StoreError::ConnectionError(format!(
                                "head_bucket: HTTP {status} service error: {:?}",
                                svc.err()
                            ))),
                        }
                    }
                    Err(e) => Err(map_sdk_err(e, "head_bucket")),
                }
            })
            .await
            .map(|_| ())
    }

    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        self.init().await?;
        let prefixed = self.prefixed_key(key);
        match self
            .client()
            .await
            .get_object()
            .bucket(&self.config.bucket)
            .key(&prefixed)
            .send()
            .await
        {
            Ok(out) => {
                let bytes = out
                    .body
                    .collect()
                    .await
                    .map_err(|e| {
                        StoreError::ConnectionError(format!("get: body read error: {e:?}"))
                    })?
                    .into_bytes();
                Ok(Some(bytes.to_vec()))
            }
            Err(SdkError::ServiceError(svc)) if svc.err().is_no_such_key() => Ok(None),
            Err(e) => Err(map_sdk_err(e, "get_object")),
        }
    }

    async fn set(&self, key: &str, value: Vec<u8>) -> Result<()> {
        self.init().await?;
        let prefixed = self.prefixed_key(key);
        self.client()
            .await
            .put_object()
            .bucket(&self.config.bucket)
            .key(&prefixed)
            .body(value.into())
            .send()
            .await
            .map_err(|e| map_sdk_err(e, "put_object"))?;
        Ok(())
    }

    async fn remove(&self, key: &str) -> Result<()> {
        self.init().await?;
        let prefixed = self.prefixed_key(key);
        // S3 DeleteObject is idempotent — succeeds even if the key doesn't exist.
        self.client()
            .await
            .delete_object()
            .bucket(&self.config.bucket)
            .key(&prefixed)
            .send()
            .await
            .map_err(|e| map_sdk_err(e, "delete_object"))?;
        Ok(())
    }

    async fn exists(&self, key: &str) -> Result<bool> {
        self.init().await?;
        let prefixed = self.prefixed_key(key);
        match self
            .client()
            .await
            .head_object()
            .bucket(&self.config.bucket)
            .key(&prefixed)
            .send()
            .await
        {
            Ok(_) => Ok(true),
            Err(SdkError::ServiceError(svc)) => {
                let status = svc.raw().status().as_u16();
                match status {
                    404 => Ok(false),
                    401 | 403 => Err(StoreError::NotAuthorized(format!(
                        "head_object: HTTP {status} from S3"
                    ))),
                    _ => Err(StoreError::ConnectionError(format!(
                        "head_object: HTTP {status} service error: {:?}",
                        svc.err()
                    ))),
                }
            }
            Err(e) => Err(map_sdk_err(e, "head_object")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn server_s3_config_round_trips_through_serde() {
        let cfg = ServerS3Config {
            bucket: "example-bucket".to_string(),
            bucket_prefix: Some("docs".to_string()),
        };
        let json = serde_json::to_string(&cfg).unwrap();
        let back: ServerS3Config = serde_json::from_str(&json).unwrap();
        assert_eq!(back.bucket, "example-bucket");
        assert_eq!(back.bucket_prefix.as_deref(), Some("docs"));
    }

    #[test]
    fn prefixed_key_with_prefix() {
        let store = ServerS3Store::new(ServerS3Config {
            bucket: "b".to_string(),
            bucket_prefix: Some("workdocs".to_string()),
        });
        assert_eq!(store.prefixed_key("doc-123"), "workdocs/doc-123");
    }

    #[test]
    fn prefixed_key_without_prefix() {
        let store = ServerS3Store::new(ServerS3Config {
            bucket: "b".to_string(),
            bucket_prefix: None,
        });
        assert_eq!(store.prefixed_key("doc-123"), "doc-123");
    }
}
