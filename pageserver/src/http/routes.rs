use std::sync::Arc;

use anyhow::{Context, Result};
use hyper::StatusCode;
use hyper::{Body, Request, Response, Uri};
use tracing::*;

use super::models::{
    StatusResponse, TenantCreateRequest, TenantCreateResponse, TimelineCreateRequest,
};
use crate::config::RemoteStorageKind;
use crate::remote_storage::{
    download_index_part, schedule_timeline_download, LocalFs, RemoteIndex, RemoteTimeline, S3Bucket,
};
use crate::repository::Repository;
use crate::timelines::{LocalTimelineInfo, RemoteTimelineInfo, TimelineInfo};
use crate::{config::PageServerConf, tenant_mgr, timelines};
use utils::{
    auth::JwtAuth,
    http::{
        endpoint::{self, attach_openapi_ui, auth_middleware, check_permission},
        error::{ApiError, HttpErrorBody},
        json::{json_request, json_response},
        request::parse_request_param,
        RequestExt, RouterBuilder,
    },
    zid::{ZTenantId, ZTenantTimelineId, ZTimelineId},
};

struct State {
    conf: &'static PageServerConf,
    auth: Option<Arc<JwtAuth>>,
    remote_index: RemoteIndex,
    allowlist_routes: Vec<Uri>,
    remote_storage: Option<GenericRemoteStorage>,
}

enum GenericRemoteStorage {
    Local(LocalFs),
    S3(S3Bucket),
}

impl State {
    fn new(
        conf: &'static PageServerConf,
        auth: Option<Arc<JwtAuth>>,
        remote_index: RemoteIndex,
    ) -> anyhow::Result<Self> {
        let allowlist_routes = ["/v1/status", "/v1/doc", "/swagger.yml"]
            .iter()
            .map(|v| v.parse().unwrap())
            .collect::<Vec<_>>();
        // Note that this remote storage is created separately from the main one in the sync_loop.
        // It's fine since it's stateless and some code duplication saves us from bloating the code around with generics.
        let remote_storage = conf
            .remote_storage_config
            .as_ref()
            .map(|storage_config| match &storage_config.storage {
                RemoteStorageKind::LocalFs(root) => {
                    LocalFs::new(root.clone(), &conf.workdir).map(GenericRemoteStorage::Local)
                }
                RemoteStorageKind::AwsS3(s3_config) => {
                    S3Bucket::new(s3_config, &conf.workdir).map(GenericRemoteStorage::S3)
                }
            })
            .transpose()
            .context("Failed to init generic remote storage")?;

        Ok(Self {
            conf,
            auth,
            allowlist_routes,
            remote_index,
            remote_storage,
        })
    }
}

#[inline(always)]
fn get_state(request: &Request<Body>) -> &State {
    request
        .data::<Arc<State>>()
        .expect("unknown state type")
        .as_ref()
}

#[inline(always)]
fn get_config(request: &Request<Body>) -> &'static PageServerConf {
    get_state(request).conf
}

// healthcheck handler
async fn status_handler(request: Request<Body>) -> Result<Response<Body>, ApiError> {
    let config = get_config(&request);
    json_response(StatusCode::OK, StatusResponse { id: config.id })
}

async fn timeline_create_handler(mut request: Request<Body>) -> Result<Response<Body>, ApiError> {
    let tenant_id: ZTenantId = parse_request_param(&request, "tenant_id")?;
    let request_data: TimelineCreateRequest = json_request(&mut request).await?;

    check_permission(&request, Some(tenant_id))?;

    let new_timeline_info = tokio::task::spawn_blocking(move || {
        let _enter = info_span!("/timeline_create", tenant = %tenant_id, new_timeline = ?request_data.new_timeline_id, lsn=?request_data.ancestor_start_lsn).entered();
        timelines::create_timeline(
            get_config(&request),
            tenant_id,
            request_data.new_timeline_id.map(ZTimelineId::from),
            request_data.ancestor_timeline_id.map(ZTimelineId::from),
            request_data.ancestor_start_lsn,
        )
    })
    .await
    .map_err(ApiError::from_err)??;

    Ok(match new_timeline_info {
        Some(info) => json_response(StatusCode::CREATED, info)?,
        None => json_response(StatusCode::CONFLICT, ())?,
    })
}

async fn timeline_list_handler(request: Request<Body>) -> Result<Response<Body>, ApiError> {
    let tenant_id: ZTenantId = parse_request_param(&request, "tenant_id")?;
    check_permission(&request, Some(tenant_id))?;
    let include_non_incremental_logical_size = get_include_non_incremental_logical_size(&request);
    let local_timeline_infos = tokio::task::spawn_blocking(move || {
        let _enter = info_span!("timeline_list", tenant = %tenant_id).entered();
        crate::timelines::get_local_timelines(tenant_id, include_non_incremental_logical_size)
    })
    .await
    .map_err(ApiError::from_err)??;

    let mut response_data = Vec::with_capacity(local_timeline_infos.len());
    for (timeline_id, local_timeline_info) in local_timeline_infos {
        response_data.push(TimelineInfo {
            tenant_id,
            timeline_id,
            local: Some(local_timeline_info),
            remote: get_state(&request)
                .remote_index
                .read()
                .await
                .timeline_entry(&ZTenantTimelineId {
                    tenant_id,
                    timeline_id,
                })
                .map(|remote_entry| RemoteTimelineInfo {
                    remote_consistent_lsn: remote_entry.metadata.disk_consistent_lsn(),
                    awaits_download: remote_entry.awaits_download,
                }),
        })
    }

    json_response(StatusCode::OK, response_data)
}

// Gate non incremental logical size calculation behind a flag
// after pgbench -i -s100 calculation took 28ms so if multiplied by the number of timelines
// and tenants it can take noticeable amount of time. Also the value currently used only in tests
fn get_include_non_incremental_logical_size(request: &Request<Body>) -> bool {
    request
        .uri()
        .query()
        .map(|v| {
            url::form_urlencoded::parse(v.as_bytes())
                .into_owned()
                .any(|(param, _)| param == "include-non-incremental-logical-size")
        })
        .unwrap_or(false)
}

async fn timeline_detail_handler(request: Request<Body>) -> Result<Response<Body>, ApiError> {
    let tenant_id: ZTenantId = parse_request_param(&request, "tenant_id")?;
    check_permission(&request, Some(tenant_id))?;

    let timeline_id: ZTimelineId = parse_request_param(&request, "timeline_id")?;
    let include_non_incremental_logical_size = get_include_non_incremental_logical_size(&request);

    let span = info_span!("timeline_detail_handler", tenant = %tenant_id, timeline = %timeline_id);

    let (local_timeline_info, span) = tokio::task::spawn_blocking(move || {
        let entered = span.entered();
        let repo = tenant_mgr::get_repository_for_tenant(tenant_id)?;
        let local_timeline = {
            repo.get_timeline(timeline_id)
                .as_ref()
                .map(|timeline| {
                    LocalTimelineInfo::from_repo_timeline(
                        tenant_id,
                        timeline_id,
                        timeline,
                        include_non_incremental_logical_size,
                    )
                })
                .transpose()?
        };
        Ok::<_, anyhow::Error>((local_timeline, entered.exit()))
    })
    .await
    .map_err(ApiError::from_err)??;

    let remote_timeline_info = {
        let remote_index_read = get_state(&request).remote_index.read().await;
        remote_index_read
            .timeline_entry(&ZTenantTimelineId {
                tenant_id,
                timeline_id,
            })
            .map(|remote_entry| RemoteTimelineInfo {
                remote_consistent_lsn: remote_entry.metadata.disk_consistent_lsn(),
                awaits_download: remote_entry.awaits_download,
            })
    };

    let _enter = span.entered();

    if local_timeline_info.is_none() && remote_timeline_info.is_none() {
        return Err(ApiError::NotFound(
            "Timeline is not found neither locally nor remotely".to_string(),
        ));
    }

    let timeline_info = TimelineInfo {
        tenant_id,
        timeline_id,
        local: local_timeline_info,
        remote: remote_timeline_info,
    };

    json_response(StatusCode::OK, timeline_info)
}

async fn timeline_attach_handler(request: Request<Body>) -> Result<Response<Body>, ApiError> {
    let tenant_id: ZTenantId = parse_request_param(&request, "tenant_id")?;
    check_permission(&request, Some(tenant_id))?;

    let timeline_id: ZTimelineId = parse_request_param(&request, "timeline_id")?;
    info!(
        "Handling timeline {} attach for tenant: {}",
        timeline_id, tenant_id,
    );

    tokio::task::spawn_blocking(move || {
        if tenant_mgr::get_timeline_for_tenant_load(tenant_id, timeline_id).is_ok() {
            // TODO: maybe answer with 309 Not Modified here?
            anyhow::bail!("Timeline is already present locally")
        };
        Ok(())
    })
    .await
    .map_err(ApiError::from_err)??;

    let sync_id = ZTenantTimelineId {
        tenant_id,
        timeline_id,
    };
    let state = get_state(&request);
    let remote_index = &state.remote_index;

    let mut index_accessor = remote_index.write().await;
    if let Some(remote_timeline) = index_accessor.timeline_entry_mut(&sync_id) {
        if remote_timeline.awaits_download {
            return Err(ApiError::Conflict(
                "Timeline download is already in progress".to_string(),
            ));
        }

        remote_timeline.awaits_download = true;
        schedule_timeline_download(tenant_id, timeline_id);
        return json_response(StatusCode::ACCEPTED, ());
    } else {
        // no timeline in the index, release the lock to make the potentially lengthy download opetation
        drop(index_accessor);
    }

    let new_timeline = match try_download_shard_data(state, sync_id).await {
        Ok(Some(mut new_timeline)) => {
            tokio::fs::create_dir_all(state.conf.timeline_path(&timeline_id, &tenant_id))
                .await
                .context("Failed to create new timeline directory")?;
            new_timeline.awaits_download = true;
            new_timeline
        }
        Ok(None) => return Err(ApiError::NotFound("Unknown remote timeline".to_string())),
        Err(e) => {
            error!("Failed to retrieve remote timeline data: {:?}", e);
            return Err(ApiError::NotFound(
                "Failed to retrieve remote timeline".to_string(),
            ));
        }
    };

    let mut index_accessor = remote_index.write().await;
    match index_accessor.timeline_entry_mut(&sync_id) {
        Some(remote_timeline) => {
            if remote_timeline.awaits_download {
                return Err(ApiError::Conflict(
                    "Timeline download is already in progress".to_string(),
                ));
            }
            remote_timeline.awaits_download = true;
        }
        None => index_accessor.add_timeline_entry(sync_id, new_timeline),
    }
    schedule_timeline_download(tenant_id, timeline_id);
    json_response(StatusCode::ACCEPTED, ())
}

async fn try_download_shard_data(
    state: &State,
    sync_id: ZTenantTimelineId,
) -> anyhow::Result<Option<RemoteTimeline>> {
    let shard = match state.remote_storage.as_ref() {
        Some(GenericRemoteStorage::Local(local_storage)) => {
            download_index_part(state.conf, local_storage, sync_id).await
        }
        Some(GenericRemoteStorage::S3(s3_storage)) => {
            download_index_part(state.conf, s3_storage, sync_id).await
        }
        None => return Ok(None),
    }
    .with_context(|| format!("Failed to download index shard for timeline {}", sync_id))?;

    let timeline_path = state
        .conf
        .timeline_path(&sync_id.timeline_id, &sync_id.tenant_id);
    RemoteTimeline::from_index_part(&timeline_path, shard)
        .map(Some)
        .with_context(|| {
            format!(
                "Failed to convert index shard into remote timeline for timeline {}",
                sync_id
            )
        })
}

async fn timeline_detach_handler(request: Request<Body>) -> Result<Response<Body>, ApiError> {
    let tenant_id: ZTenantId = parse_request_param(&request, "tenant_id")?;
    check_permission(&request, Some(tenant_id))?;

    let timeline_id: ZTimelineId = parse_request_param(&request, "timeline_id")?;

    tokio::task::spawn_blocking(move || {
        let _enter =
            info_span!("timeline_detach_handler", tenant = %tenant_id, timeline = %timeline_id)
                .entered();
        let repo = tenant_mgr::get_repository_for_tenant(tenant_id)?;
        repo.detach_timeline(timeline_id)
    })
    .await
    .map_err(ApiError::from_err)??;

    json_response(StatusCode::OK, ())
}

async fn tenant_list_handler(request: Request<Body>) -> Result<Response<Body>, ApiError> {
    // check for management permission
    check_permission(&request, None)?;

    let response_data = tokio::task::spawn_blocking(move || {
        let _enter = info_span!("tenant_list").entered();
        crate::tenant_mgr::list_tenants()
    })
    .await
    .map_err(ApiError::from_err)??;

    json_response(StatusCode::OK, response_data)
}

async fn tenant_create_handler(mut request: Request<Body>) -> Result<Response<Body>, ApiError> {
    // check for management permission
    check_permission(&request, None)?;

    let request_data: TenantCreateRequest = json_request(&mut request).await?;
    let remote_index = get_state(&request).remote_index.clone();

    let target_tenant_id = request_data
        .new_tenant_id
        .map(ZTenantId::from)
        .unwrap_or_else(ZTenantId::generate);

    let new_tenant_id = tokio::task::spawn_blocking(move || {
        let _enter = info_span!("tenant_create", tenant = ?target_tenant_id).entered();

        tenant_mgr::create_tenant_repository(get_config(&request), target_tenant_id, remote_index)
    })
    .await
    .map_err(ApiError::from_err)??;

    Ok(match new_tenant_id {
        Some(id) => json_response(StatusCode::CREATED, TenantCreateResponse(id))?,
        None => json_response(StatusCode::CONFLICT, ())?,
    })
}

async fn handler_404(_: Request<Body>) -> Result<Response<Body>, ApiError> {
    json_response(
        StatusCode::NOT_FOUND,
        HttpErrorBody::from_msg("page not found".to_owned()),
    )
}

pub fn make_router(
    conf: &'static PageServerConf,
    auth: Option<Arc<JwtAuth>>,
    remote_index: RemoteIndex,
) -> anyhow::Result<RouterBuilder<hyper::Body, ApiError>> {
    let spec = include_bytes!("openapi_spec.yml");
    let mut router = attach_openapi_ui(endpoint::make_router(), spec, "/swagger.yml", "/v1/doc");
    if auth.is_some() {
        router = router.middleware(auth_middleware(|request| {
            let state = get_state(request);
            if state.allowlist_routes.contains(request.uri()) {
                None
            } else {
                state.auth.as_deref()
            }
        }))
    }

    Ok(router
        .data(Arc::new(
            State::new(conf, auth, remote_index).context("Failed to initialize router state")?,
        ))
        .get("/v1/status", status_handler)
        .get("/v1/tenant", tenant_list_handler)
        .post("/v1/tenant", tenant_create_handler)
        .get("/v1/tenant/:tenant_id/timeline", timeline_list_handler)
        .post("/v1/tenant/:tenant_id/timeline", timeline_create_handler)
        .get(
            "/v1/tenant/:tenant_id/timeline/:timeline_id",
            timeline_detail_handler,
        )
        .post(
            "/v1/tenant/:tenant_id/timeline/:timeline_id/attach",
            timeline_attach_handler,
        )
        .post(
            "/v1/tenant/:tenant_id/timeline/:timeline_id/detach",
            timeline_detach_handler,
        )
        .any(handler_404))
}
