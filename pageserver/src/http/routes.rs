use std::sync::Arc;

use anyhow::{Context, Result};
use hyper::StatusCode;
use hyper::{Body, Request, Response, Uri};
use remote_storage::GenericRemoteStorage;
use tracing::*;

use super::models::{
    StatusResponse, TenantConfigRequest, TenantCreateRequest, TenantCreateResponse,
    TimelineCreateRequest,
};
use crate::repository::Repository;
use crate::storage_sync;
use crate::storage_sync::index::{RemoteIndex, RemoteTimeline};
use crate::tenant_config::TenantConfOpt;
use crate::tenant_mgr::TenantInfo;
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
            .map(|storage_config| GenericRemoteStorage::new(conf.workdir.clone(), storage_config))
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

    let (local_timeline_info, remote_timeline_info) = async {
        // any error here will render local timeline as None
        // XXX .in_current_span does not attach messages in spawn_blocking future to current future's span
        let local_timeline_info = tokio::task::spawn_blocking(move || {
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
            Ok::<_, anyhow::Error>(local_timeline)
        })
        .await
        .ok()
        .and_then(|r| r.ok())
        .flatten();

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
        (local_timeline_info, remote_timeline_info)
    }
    .instrument(info_span!("timeline_detail_handler", tenant = %tenant_id, timeline = %timeline_id))
    .await;

    if local_timeline_info.is_none() && remote_timeline_info.is_none() {
        return Err(ApiError::NotFound(format!(
            "Timeline {tenant_id}/{timeline_id} is not found neither locally nor remotely"
        )));
    }

    let timeline_info = TimelineInfo {
        tenant_id,
        timeline_id,
        local: local_timeline_info,
        remote: remote_timeline_info,
    };

    json_response(StatusCode::OK, timeline_info)
}

async fn wal_receiver_get_handler(request: Request<Body>) -> Result<Response<Body>, ApiError> {
    let tenant_id: ZTenantId = parse_request_param(&request, "tenant_id")?;
    check_permission(&request, Some(tenant_id))?;

    let timeline_id: ZTimelineId = parse_request_param(&request, "timeline_id")?;
    let wal_receiver_entry = crate::walreceiver::get_wal_receiver_entry(tenant_id, timeline_id)
        .instrument(info_span!("wal_receiver_get", tenant = %tenant_id, timeline = %timeline_id))
        .await
        .ok_or_else(|| {
            ApiError::NotFound(format!(
                "WAL receiver data not found for tenant {tenant_id} and timeline {timeline_id}"
            ))
        })?;

    json_response(StatusCode::OK, &wal_receiver_entry)
}

// TODO makes sense to provide tenant config right away the same way as it handled in tenant_create
async fn tenant_attach_handler(request: Request<Body>) -> Result<Response<Body>, ApiError> {
    let tenant_id: ZTenantId = parse_request_param(&request, "tenant_id")?;
    check_permission(&request, Some(tenant_id))?;

    info!("Handling tenant attach {}", tenant_id,);

    tokio::task::spawn_blocking(move || {
        if tenant_mgr::get_tenant_state(tenant_id).is_some() {
            anyhow::bail!("Tenant is already present locally")
        };
        Ok(())
    })
    .await
    .map_err(ApiError::from_err)??;

    let state = get_state(&request);
    let remote_index = &state.remote_index;

    let mut index_accessor = remote_index.write().await;
    if let Some(tenant_entry) = index_accessor.tenant_entry_mut(&tenant_id) {
        if tenant_entry.has_in_progress_downloads() {
            return Err(ApiError::Conflict(
                "Tenant download is already in progress".to_string(),
            ));
        }

        for (timeline_id, remote_timeline) in tenant_entry.iter_mut() {
            storage_sync::schedule_layer_download(tenant_id, *timeline_id);
            remote_timeline.awaits_download = true;
        }
        return json_response(StatusCode::ACCEPTED, ());
    }
    // no tenant in the index, release the lock to make the potentially lengthy download opetation
    drop(index_accessor);

    // download index parts for every tenant timeline
    let remote_timelines = match gather_tenant_timelines_index_parts(state, tenant_id).await {
        Ok(Some(remote_timelines)) => remote_timelines,
        Ok(None) => return Err(ApiError::NotFound("Unknown remote tenant".to_string())),
        Err(e) => {
            error!("Failed to retrieve remote tenant data: {:?}", e);
            return Err(ApiError::NotFound(
                "Failed to retrieve remote tenant".to_string(),
            ));
        }
    };

    // recheck that download is not in progress because
    // we've released the lock to avoid holding it during the download
    let mut index_accessor = remote_index.write().await;
    let tenant_entry = match index_accessor.tenant_entry_mut(&tenant_id) {
        Some(tenant_entry) => {
            if tenant_entry.has_in_progress_downloads() {
                return Err(ApiError::Conflict(
                    "Tenant download is already in progress".to_string(),
                ));
            }
            tenant_entry
        }
        None => index_accessor.add_tenant_entry(tenant_id),
    };

    // populate remote index with the data from index part and create directories on the local filesystem
    for (timeline_id, mut remote_timeline) in remote_timelines {
        tokio::fs::create_dir_all(state.conf.timeline_path(&timeline_id, &tenant_id))
            .await
            .context("Failed to create new timeline directory")?;

        remote_timeline.awaits_download = true;
        tenant_entry.insert(timeline_id, remote_timeline);
        // schedule actual download
        storage_sync::schedule_layer_download(tenant_id, timeline_id);
    }

    json_response(StatusCode::ACCEPTED, ())
}

/// Note: is expensive from s3 access perspective,
/// for details see comment to `storage_sync::gather_tenant_timelines_index_parts`
async fn gather_tenant_timelines_index_parts(
    state: &State,
    tenant_id: ZTenantId,
) -> anyhow::Result<Option<Vec<(ZTimelineId, RemoteTimeline)>>> {
    let index_parts = match state.remote_storage.as_ref() {
        Some(GenericRemoteStorage::Local(local_storage)) => {
            storage_sync::gather_tenant_timelines_index_parts(state.conf, local_storage, tenant_id)
                .await
        }
        // FIXME here s3 storage contains its own limits, that are separate from sync storage thread ones
        //       because it is a different instance. We can move this limit to some global static
        //       or use one instance everywhere.
        Some(GenericRemoteStorage::S3(s3_storage)) => {
            storage_sync::gather_tenant_timelines_index_parts(state.conf, s3_storage, tenant_id)
                .await
        }
        None => return Ok(None),
    }
    .with_context(|| format!("Failed to download index parts for tenant {tenant_id}"))?;

    let mut remote_timelines = Vec::with_capacity(index_parts.len());
    for (timeline_id, index_part) in index_parts {
        let timeline_path = state.conf.timeline_path(&timeline_id, &tenant_id);
        let remote_timeline = RemoteTimeline::from_index_part(&timeline_path, index_part)
            .with_context(|| {
                format!("Failed to convert index part into remote timeline for timeline {tenant_id}/{timeline_id}")
            })?;
        remote_timelines.push((timeline_id, remote_timeline));
    }
    Ok(Some(remote_timelines))
}

async fn timeline_delete_handler(request: Request<Body>) -> Result<Response<Body>, ApiError> {
    let tenant_id: ZTenantId = parse_request_param(&request, "tenant_id")?;
    check_permission(&request, Some(tenant_id))?;

    let timeline_id: ZTimelineId = parse_request_param(&request, "timeline_id")?;

    let state = get_state(&request);
    tokio::task::spawn_blocking(move || {
        let _enter = info_span!("tenant_detach_handler", tenant = %tenant_id).entered();
        tenant_mgr::delete_timeline(tenant_id, timeline_id)
    })
    .await
    .map_err(ApiError::from_err)??;

    let mut remote_index = state.remote_index.write().await;
    remote_index.remove_timeline_entry(ZTenantTimelineId {
        tenant_id,
        timeline_id,
    });

    json_response(StatusCode::OK, ())
}

async fn tenant_detach_handler(request: Request<Body>) -> Result<Response<Body>, ApiError> {
    let tenant_id: ZTenantId = parse_request_param(&request, "tenant_id")?;
    check_permission(&request, Some(tenant_id))?;

    let state = get_state(&request);
    let conf = state.conf;
    tokio::task::spawn_blocking(move || {
        let _enter = info_span!("tenant_detach_handler", tenant = %tenant_id).entered();
        tenant_mgr::detach_tenant(conf, tenant_id)
    })
    .await
    .map_err(ApiError::from_err)??;

    let mut remote_index = state.remote_index.write().await;
    remote_index.remove_tenant_entry(&tenant_id);

    json_response(StatusCode::OK, ())
}

async fn tenant_list_handler(request: Request<Body>) -> Result<Response<Body>, ApiError> {
    // check for management permission
    check_permission(&request, None)?;

    let state = get_state(&request);
    // clone to avoid holding the lock while awaiting for blocking task
    let remote_index = state.remote_index.read().await.clone();

    let response_data = tokio::task::spawn_blocking(move || {
        let _enter = info_span!("tenant_list").entered();
        crate::tenant_mgr::list_tenants(&remote_index)
    })
    .await
    .map_err(ApiError::from_err)?;

    json_response(StatusCode::OK, response_data)
}

async fn tenant_status(request: Request<Body>) -> Result<Response<Body>, ApiError> {
    let tenant_id: ZTenantId = parse_request_param(&request, "tenant_id")?;
    check_permission(&request, Some(tenant_id))?;

    // if tenant is in progress of downloading it can be absent in global tenant map
    let tenant_state = tokio::task::spawn_blocking(move || tenant_mgr::get_tenant_state(tenant_id))
        .await
        .map_err(ApiError::from_err)?;

    let state = get_state(&request);
    let remote_index = &state.remote_index;

    let index_accessor = remote_index.read().await;
    let has_in_progress_downloads = index_accessor
        .tenant_entry(&tenant_id)
        .ok_or_else(|| ApiError::NotFound("Tenant not found in remote index".to_string()))?
        .has_in_progress_downloads();

    json_response(
        StatusCode::OK,
        TenantInfo {
            id: tenant_id,
            state: tenant_state,
            has_in_progress_downloads: Some(has_in_progress_downloads),
        },
    )
}

async fn tenant_create_handler(mut request: Request<Body>) -> Result<Response<Body>, ApiError> {
    // check for management permission
    check_permission(&request, None)?;

    let request_data: TenantCreateRequest = json_request(&mut request).await?;
    let remote_index = get_state(&request).remote_index.clone();

    let mut tenant_conf = TenantConfOpt::default();
    if let Some(gc_period) = request_data.gc_period {
        tenant_conf.gc_period =
            Some(humantime::parse_duration(&gc_period).map_err(ApiError::from_err)?);
    }
    tenant_conf.gc_horizon = request_data.gc_horizon;
    tenant_conf.image_creation_threshold = request_data.image_creation_threshold;

    if let Some(pitr_interval) = request_data.pitr_interval {
        tenant_conf.pitr_interval =
            Some(humantime::parse_duration(&pitr_interval).map_err(ApiError::from_err)?);
    }

    if let Some(walreceiver_connect_timeout) = request_data.walreceiver_connect_timeout {
        tenant_conf.walreceiver_connect_timeout = Some(
            humantime::parse_duration(&walreceiver_connect_timeout).map_err(ApiError::from_err)?,
        );
    }
    if let Some(lagging_wal_timeout) = request_data.lagging_wal_timeout {
        tenant_conf.lagging_wal_timeout =
            Some(humantime::parse_duration(&lagging_wal_timeout).map_err(ApiError::from_err)?);
    }
    if let Some(max_lsn_wal_lag) = request_data.max_lsn_wal_lag {
        tenant_conf.max_lsn_wal_lag = Some(max_lsn_wal_lag);
    }

    tenant_conf.checkpoint_distance = request_data.checkpoint_distance;
    tenant_conf.compaction_target_size = request_data.compaction_target_size;
    tenant_conf.compaction_threshold = request_data.compaction_threshold;

    if let Some(compaction_period) = request_data.compaction_period {
        tenant_conf.compaction_period =
            Some(humantime::parse_duration(&compaction_period).map_err(ApiError::from_err)?);
    }

    let target_tenant_id = request_data
        .new_tenant_id
        .map(ZTenantId::from)
        .unwrap_or_else(ZTenantId::generate);

    let new_tenant_id = tokio::task::spawn_blocking(move || {
        let _enter = info_span!("tenant_create", tenant = ?target_tenant_id).entered();
        let conf = get_config(&request);

        tenant_mgr::create_tenant_repository(conf, tenant_conf, target_tenant_id, remote_index)
    })
    .await
    .map_err(ApiError::from_err)??;

    Ok(match new_tenant_id {
        Some(id) => json_response(StatusCode::CREATED, TenantCreateResponse(id))?,
        None => json_response(StatusCode::CONFLICT, ())?,
    })
}

async fn tenant_config_handler(mut request: Request<Body>) -> Result<Response<Body>, ApiError> {
    let request_data: TenantConfigRequest = json_request(&mut request).await?;
    let tenant_id = request_data.tenant_id;
    // check for management permission
    check_permission(&request, Some(tenant_id))?;

    let mut tenant_conf: TenantConfOpt = Default::default();
    if let Some(gc_period) = request_data.gc_period {
        tenant_conf.gc_period =
            Some(humantime::parse_duration(&gc_period).map_err(ApiError::from_err)?);
    }
    tenant_conf.gc_horizon = request_data.gc_horizon;
    tenant_conf.image_creation_threshold = request_data.image_creation_threshold;

    if let Some(pitr_interval) = request_data.pitr_interval {
        tenant_conf.pitr_interval =
            Some(humantime::parse_duration(&pitr_interval).map_err(ApiError::from_err)?);
    }
    if let Some(walreceiver_connect_timeout) = request_data.walreceiver_connect_timeout {
        tenant_conf.walreceiver_connect_timeout = Some(
            humantime::parse_duration(&walreceiver_connect_timeout).map_err(ApiError::from_err)?,
        );
    }
    if let Some(lagging_wal_timeout) = request_data.lagging_wal_timeout {
        tenant_conf.lagging_wal_timeout =
            Some(humantime::parse_duration(&lagging_wal_timeout).map_err(ApiError::from_err)?);
    }
    if let Some(max_lsn_wal_lag) = request_data.max_lsn_wal_lag {
        tenant_conf.max_lsn_wal_lag = Some(max_lsn_wal_lag);
    }

    tenant_conf.checkpoint_distance = request_data.checkpoint_distance;
    tenant_conf.compaction_target_size = request_data.compaction_target_size;
    tenant_conf.compaction_threshold = request_data.compaction_threshold;

    if let Some(compaction_period) = request_data.compaction_period {
        tenant_conf.compaction_period =
            Some(humantime::parse_duration(&compaction_period).map_err(ApiError::from_err)?);
    }

    tokio::task::spawn_blocking(move || {
        let _enter = info_span!("tenant_config", tenant = ?tenant_id).entered();

        tenant_mgr::update_tenant_config(tenant_conf, tenant_id)
    })
    .await
    .map_err(ApiError::from_err)??;

    json_response(StatusCode::OK, ())
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
        .get("/v1/tenant/:tenant_id", tenant_status)
        .put("/v1/tenant/config", tenant_config_handler)
        .get("/v1/tenant/:tenant_id/timeline", timeline_list_handler)
        .post("/v1/tenant/:tenant_id/timeline", timeline_create_handler)
        .post("/v1/tenant/:tenant_id/attach", tenant_attach_handler)
        .post("/v1/tenant/:tenant_id/detach", tenant_detach_handler)
        .get(
            "/v1/tenant/:tenant_id/timeline/:timeline_id",
            timeline_detail_handler,
        )
        .delete(
            "/v1/tenant/:tenant_id/timeline/:timeline_id",
            timeline_delete_handler,
        )
        // for backward compatibility
        .post(
            "/v1/tenant/:tenant_id/timeline/:timeline_id/detach",
            timeline_delete_handler,
        )
        .get(
            "/v1/tenant/:tenant_id/timeline/:timeline_id/wal_receiver",
            wal_receiver_get_handler,
        )
        .any(handler_404))
}
